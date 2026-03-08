use std::collections::HashMap;
use std::env;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::Config as S3Config;
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::primitives::{ByteStream, Length};
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use log::{error, info};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use russh::keys::load_secret_key;
use russh::keys::ssh_key;
use russh::keys::ssh_key::LineEnding;
use russh::keys::ssh_key::rand_core::OsRng;
use russh::server::{Auth, Msg, Server as _, Session};
use russh::{Channel, ChannelId};
use russh_sftp::protocol::{
    Attrs, Data, File, FileAttributes, Handle, Name, OpenFlags, Status, StatusCode,
};
use tempfile::{Builder as TempFileBuilder, TempPath};
use tokio::fs::{self, File as TokioFile};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio::sync::Mutex;
use tokio::task::JoinSet;

const COPY_SOURCE_SET: &AsciiSet = &NON_ALPHANUMERIC.remove(b'/');
const DEFAULT_READ_AHEAD_CHUNK_SIZE: u32 = 4 * 1024 * 1024;
const DEFAULT_MULTIPART_THRESHOLD: u64 = 64 * 1024 * 1024;
const DEFAULT_MULTIPART_CHUNK_SIZE: u64 = 16 * 1024 * 1024;
const DEFAULT_MULTIPART_UPLOAD_CONCURRENCY: usize = 8;
const DEFAULT_STREAM_BUFFER_SIZE: usize = 256 * 1024;
const MIN_MULTIPART_CHUNK_SIZE: u64 = 5 * 1024 * 1024;
const DEFAULT_HOST_KEY_PATH: &str = ".sftp-rustfs-host-key";
const SSH_WINDOW_SIZE: u32 = 16 * 1024 * 1024;
const SSH_MAX_PACKET_SIZE: u32 = 65_535;

#[derive(Clone, Copy)]
struct TransferTuning {
    read_ahead_chunk_size: u32,
    multipart_threshold: u64,
    multipart_chunk_size: u64,
    multipart_upload_concurrency: usize,
    stream_buffer_size: usize,
}

impl TransferTuning {
    fn from_env() -> Self {
        Self {
            read_ahead_chunk_size: env::var("SFTP_READ_AHEAD_BYTES")
                .ok()
                .and_then(|value| value.parse().ok())
                .filter(|value: &u32| *value > 0)
                .unwrap_or(DEFAULT_READ_AHEAD_CHUNK_SIZE),
            multipart_threshold: env::var("SFTP_MULTIPART_THRESHOLD_BYTES")
                .ok()
                .and_then(|value| value.parse().ok())
                .filter(|value: &u64| *value >= MIN_MULTIPART_CHUNK_SIZE)
                .unwrap_or(DEFAULT_MULTIPART_THRESHOLD),
            multipart_chunk_size: env::var("SFTP_MULTIPART_CHUNK_BYTES")
                .ok()
                .and_then(|value| value.parse().ok())
                .map(|value: u64| value.max(MIN_MULTIPART_CHUNK_SIZE))
                .unwrap_or(DEFAULT_MULTIPART_CHUNK_SIZE),
            multipart_upload_concurrency: env::var("SFTP_MULTIPART_CONCURRENCY")
                .ok()
                .and_then(|value| value.parse().ok())
                .filter(|value: &usize| *value > 0)
                .unwrap_or(DEFAULT_MULTIPART_UPLOAD_CONCURRENCY),
            stream_buffer_size: env::var("SFTP_STREAM_BUFFER_BYTES")
                .ok()
                .and_then(|value| value.parse().ok())
                .filter(|value: &usize| *value > 0)
                .unwrap_or(DEFAULT_STREAM_BUFFER_SIZE),
        }
    }
}

#[derive(Clone)]
struct AppConfig {
    bind_addr: String,
    sftp_user: String,
    sftp_password: String,
    rustfs_endpoint: String,
    rustfs_region: String,
    rustfs_access_key: String,
    rustfs_secret_key: String,
    visible_buckets: Option<Vec<String>>,
    default_prefix: String,
    host_key_path: PathBuf,
    transfer_tuning: TransferTuning,
}

impl AppConfig {
    fn required_env(name: &'static str) -> Result<String> {
        let value = env::var(name).with_context(|| format!("{name} must be set"))?;
        let trimmed = value.trim();

        if trimmed.is_empty() {
            return Err(anyhow!("{name} must not be empty"));
        }

        Ok(trimmed.to_string())
    }

    fn from_env() -> Result<Self> {
        let visible_buckets = env::var("RUSTFS_VISIBLE_BUCKETS")
            .ok()
            .map(|raw| {
                raw.split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(ToOwned::to_owned)
                    .collect::<Vec<_>>()
            })
            .filter(|items| !items.is_empty());

        Ok(Self {
            bind_addr: env::var("SFTP_BIND").unwrap_or_else(|_| "0.0.0.0:2222".to_string()),
            sftp_user: env::var("SFTP_USER").unwrap_or_else(|_| "sftp".to_string()),
            sftp_password: Self::required_env("SFTP_PASSWORD")?,
            rustfs_endpoint: env::var("RUSTFS_ENDPOINT")
                .unwrap_or_else(|_| "http://127.0.0.1:9000".to_string()),
            rustfs_region: env::var("RUSTFS_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
            rustfs_access_key: Self::required_env("RUSTFS_ACCESS_KEY")?,
            rustfs_secret_key: Self::required_env("RUSTFS_SECRET_KEY")?,
            visible_buckets,
            default_prefix: env::var("RUSTFS_PREFIX").unwrap_or_default(),
            host_key_path: env::var("SFTP_HOST_KEY")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from(DEFAULT_HOST_KEY_PATH)),
            transfer_tuning: TransferTuning::from_env(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SftpPath {
    Root,
    Bucket {
        bucket: String,
    },
    Object {
        bucket: String,
        key: String,
    },
}

#[derive(Clone)]
struct Backend {
    client: S3Client,
    visible_buckets: Option<Vec<String>>,
    default_prefix: String,
    tuning: TransferTuning,
}

impl Backend {
    async fn new(cfg: &AppConfig) -> Result<Self> {
        let credentials = Credentials::new(
            cfg.rustfs_access_key.clone(),
            cfg.rustfs_secret_key.clone(),
            None,
            None,
            "static",
        );

        let config = S3Config::builder()
            .credentials_provider(credentials)
            .region(Region::new(cfg.rustfs_region.clone()))
            .endpoint_url(cfg.rustfs_endpoint.clone())
            .force_path_style(true)
            .behavior_version_latest()
            .build();

        let client = S3Client::from_conf(config);

        let backend = Self {
            client,
            visible_buckets: cfg.visible_buckets.clone(),
            default_prefix: cfg.default_prefix.trim_matches('/').to_string(),
            tuning: cfg.transfer_tuning,
        };

        let _ = backend.list_buckets().await?;

        Ok(backend)
    }

    fn normalize_path(path: &str) -> Result<String, StatusCode> {
        if path.is_empty() || path == "." || path == "/" {
            return Ok(String::new());
        }

        let mut parts = Vec::new();
        for part in path.split('/') {
            match part {
                "" | "." => {}
                ".." => return Err(StatusCode::PermissionDenied),
                p => parts.push(p),
            }
        }

        Ok(parts.join("/"))
    }

    fn split_path(path: &str) -> Result<SftpPath, StatusCode> {
        let normalized = Self::normalize_path(path)?;
        if normalized.is_empty() {
            return Ok(SftpPath::Root);
        }

        let mut parts = normalized.splitn(2, '/');
        let bucket = parts.next().unwrap().to_string();

        match parts.next() {
            None => Ok(SftpPath::Bucket { bucket }),
            Some(rest) if rest.is_empty() => Ok(SftpPath::Bucket { bucket }),
            Some(rest) => Ok(SftpPath::Object {
                bucket,
                key: rest.to_string(),
            }),
        }
    }

    fn qualify_object_key(&self, key: &str) -> String {
        if self.default_prefix.is_empty() {
            key.to_string()
        } else if key.is_empty() {
            self.default_prefix.clone()
        } else {
            format!("{}/{}", self.default_prefix, key)
        }
    }

    fn qualify_dir_prefix(&self, key: &str) -> String {
        let trimmed = key.trim_matches('/');
        if self.default_prefix.is_empty() {
            if trimmed.is_empty() {
                String::new()
            } else {
                format!("{trimmed}/")
            }
        } else if trimmed.is_empty() {
            format!("{}/", self.default_prefix)
        } else {
            format!("{}/{trimmed}/", self.default_prefix)
        }
    }

    fn bucket_allowed(&self, bucket: &str) -> bool {
        self.visible_buckets
            .as_ref()
            .map(|items| items.iter().any(|item| item == bucket))
            .unwrap_or(true)
    }

    async fn bucket_exists(&self, bucket: &str) -> Result<bool, StatusCode> {
        if !self.bucket_allowed(bucket) {
            return Ok(false);
        }

        match self.client.head_bucket().bucket(bucket).send().await {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    async fn list_buckets(&self) -> Result<Vec<String>, StatusCode> {
        let resp = self
            .client
            .list_buckets()
            .send()
            .await
            .map_err(|_| StatusCode::Failure)?;

        let mut buckets = resp
            .buckets()
            .iter()
            .filter_map(|bucket| bucket.name().map(ToOwned::to_owned))
            .filter(|bucket| self.bucket_allowed(bucket))
            .collect::<Vec<_>>();

        buckets.sort();
        Ok(buckets)
    }

    fn ok_status(id: u32) -> Status {
        Status {
            id,
            status_code: StatusCode::Ok,
            error_message: "Ok".to_string(),
            language_tag: "en-US".to_string(),
        }
    }

    fn dir_attrs() -> FileAttributes {
        let mut attrs = FileAttributes::empty();
        attrs.permissions = Some(0o040755);
        attrs.set_dir(true);
        attrs
    }

    fn file_attrs(size: u64) -> FileAttributes {
        let mut attrs = FileAttributes::empty();
        attrs.size = Some(size);
        attrs.permissions = Some(0o100644);
        attrs.set_regular(true);
        attrs
    }

    async fn root_entries(&self) -> Result<Vec<File>, StatusCode> {
        let buckets = self.list_buckets().await?;
        Ok(buckets
            .into_iter()
            .map(|bucket| File::new(bucket, Self::dir_attrs()))
            .collect())
    }

    async fn stat_path(&self, path: &str) -> Result<FileAttributes, StatusCode> {
        match Self::split_path(path)? {
            SftpPath::Root => Ok(Self::dir_attrs()),
            SftpPath::Bucket { bucket } => {
                if self.bucket_exists(&bucket).await? {
                    Ok(Self::dir_attrs())
                } else {
                    Err(StatusCode::NoSuchFile)
                }
            }
            SftpPath::Object { bucket, key } => {
                if !self.bucket_exists(&bucket).await? {
                    return Err(StatusCode::NoSuchFile);
                }

                let object_key = self.qualify_object_key(&key);
                if let Ok(head) = self
                    .client
                    .head_object()
                    .bucket(&bucket)
                    .key(&object_key)
                    .send()
                    .await
                {
                    return Ok(Self::file_attrs(
                          head.content_length().unwrap_or_default().max(0) as u64,
            ));
                }

                let dir_prefix = self.qualify_dir_prefix(&key);
                let list = self
                    .client
                    .list_objects_v2()
                    .bucket(&bucket)
                    .prefix(dir_prefix.clone())
                    .delimiter("/")
                    .max_keys(1)
                    .send()
                    .await
                    .map_err(|_| StatusCode::Failure)?;

                let has_entries = !list.contents().is_empty()
                    || list
                        .common_prefixes()
                        .iter()
                        .any(|prefix| prefix.prefix().is_some());

                if has_entries {
                    Ok(Self::dir_attrs())
                } else {
                    Err(StatusCode::NoSuchFile)
                }
            }
        }
    }

    async fn list_dir(&self, path: &str) -> Result<Vec<File>, StatusCode> {
        match Self::split_path(path)? {
            SftpPath::Root => self.root_entries().await,
            SftpPath::Bucket { bucket } => {
                if !self.bucket_exists(&bucket).await? {
                    return Err(StatusCode::NoSuchFile);
                }
                self.list_bucket_dir(&bucket, "").await
            }
            SftpPath::Object { bucket, key } => {
                if !self.bucket_exists(&bucket).await? {
                    return Err(StatusCode::NoSuchFile);
                }

                let attrs = self.stat_path(path).await?;
                if !attrs.is_dir() {
                    return Err(StatusCode::Failure);
                }

                self.list_bucket_dir(&bucket, &key).await
            }
        }
    }

    async fn list_bucket_dir(&self, bucket: &str, key: &str) -> Result<Vec<File>, StatusCode> {
        let request_prefix = self.qualify_dir_prefix(key);

        let resp = self
            .client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(request_prefix.clone())
            .delimiter("/")
            .send()
            .await
            .map_err(|_| StatusCode::Failure)?;

        let mut files = Vec::new();

        for prefix in resp.common_prefixes() {
            if let Some(full) = prefix.prefix() {
                let short = full
                    .strip_prefix(&request_prefix)
                    .unwrap_or(full)
                    .trim_end_matches('/');
                if !short.is_empty() && !short.contains('/') {
                    files.push(File::new(short.to_string(), Self::dir_attrs()));
                }
            }
        }

        for object in resp.contents() {
            let Some(full) = object.key() else {
                continue;
            };

            if full == request_prefix {
                continue;
            }

            let short = full.strip_prefix(&request_prefix).unwrap_or(full);
            if !short.is_empty() && !short.contains('/') {
                files.push(File::new(
                    short.to_string(),
                    Self::file_attrs(object.size().unwrap_or_default().max(0) as u64),
                ));
            }
        }

        Ok(files)
    }

    async fn head_object(&self, bucket: &str, key: &str) -> Result<u64, StatusCode> {
        let head = self
            .client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|_| StatusCode::NoSuchFile)?;

        Ok(head.content_length().unwrap_or_default().max(0) as u64)
    }

    async fn read_object_range(
        &self,
        bucket: &str,
        key: &str,
        offset: u64,
        len: u32,
        object_size: u64,
    ) -> Result<Vec<u8>, StatusCode> {
        if offset >= object_size {
            return Err(StatusCode::Eof);
        }

        let max_len = len.max(1) as u64;
        let end = (offset + max_len - 1).min(object_size - 1);
        let range = format!("bytes={offset}-{end}");

        let resp = self
            .client
            .get_object()
            .bucket(bucket)
            .key(key)
            .range(range)
            .send()
            .await
            .map_err(|_| StatusCode::Failure)?;

        let bytes = resp
            .body
            .collect()
            .await
            .map_err(|_| StatusCode::Failure)?
            .into_bytes();

        Ok(bytes.to_vec())
    }

    async fn seed_local_file_from_object(
        &self,
        bucket: &str,
        key: &str,
        file: &mut TokioFile,
    ) -> Result<(), StatusCode> {
        let resp = self
            .client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|_| StatusCode::NoSuchFile)?;

        let mut reader = resp.body.into_async_read();
        tokio::io::copy(&mut reader, file)
            .await
            .map_err(|_| StatusCode::Failure)?;
        file.seek(SeekFrom::Start(0))
            .await
            .map_err(|_| StatusCode::Failure)?;

        Ok(())
    }

    async fn put_object_from_path(
        &self,
        bucket: &str,
        key: &str,
        temp_path: &Path,
        object_size: u64,
    ) -> Result<(), StatusCode> {
        let body = ByteStream::read_from()
            .path(temp_path)
            .buffer_size(self.tuning.stream_buffer_size)
            .length(Length::Exact(object_size))
            .build()
            .await
            .map_err(|_| StatusCode::Failure)?;

        self.client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(body)
            .send()
            .await
            .map_err(|_| StatusCode::Failure)?;

        Ok(())
    }

    async fn multipart_upload_from_path(
        &self,
        bucket: &str,
        key: &str,
        temp_path: &Path,
        object_size: u64,
    ) -> Result<(), StatusCode> {
        let create = self
            .client
            .create_multipart_upload()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|_| StatusCode::Failure)?;

        let upload_id = create
            .upload_id()
            .ok_or(StatusCode::Failure)?
            .to_string();

        let result = async {
            let mut part_number = 1_i32;
            let mut offset = 0_u64;
            let mut uploads = JoinSet::new();
            let mut completed_parts = Vec::new();

            while offset < object_size {
                let part_len = (object_size - offset).min(self.tuning.multipart_chunk_size);
                if part_len == 0 {
                    break;
                }

                let client = self.client.clone();
                let upload_id = upload_id.clone();
                let bucket = bucket.to_string();
                let key = key.to_string();
                let path = temp_path.to_path_buf();
                let current_part_number = part_number;
                let current_offset = offset;
                let stream_buffer_size = self.tuning.stream_buffer_size;

                uploads.spawn(async move {
                    let body = ByteStream::read_from()
                        .path(path)
                        .offset(current_offset)
                        .length(Length::Exact(part_len))
                        .buffer_size(stream_buffer_size)
                        .build()
                        .await
                        .map_err(|_| StatusCode::Failure)?;

                    let uploaded = client
                        .upload_part()
                        .bucket(bucket)
                        .key(key)
                        .upload_id(upload_id)
                        .part_number(current_part_number)
                        .body(body)
                        .send()
                        .await
                        .map_err(|_| StatusCode::Failure)?;

                    let etag = uploaded.e_tag().ok_or(StatusCode::Failure)?.to_string();
                    Ok::<(i32, String), StatusCode>((current_part_number, etag))
                });

                if uploads.len() >= self.tuning.multipart_upload_concurrency {
                    let upload_result = uploads.join_next().await.ok_or(StatusCode::Failure)?;
                    let (part_number, etag) = upload_result.map_err(|_| StatusCode::Failure)??;
                    completed_parts.push((part_number, etag));
                }

                offset += part_len;
                part_number += 1;
            }

            while let Some(upload_result) = uploads.join_next().await {
                let (part_number, etag) = upload_result.map_err(|_| StatusCode::Failure)??;
                completed_parts.push((part_number, etag));
            }

            completed_parts.sort_by_key(|(part_number, _)| *part_number);

            let multipart = CompletedMultipartUpload::builder()
                .set_parts(Some(
                    completed_parts
                        .into_iter()
                        .map(|(part_number, etag)| {
                            CompletedPart::builder()
                                .part_number(part_number)
                                .e_tag(etag)
                                .build()
                        })
                        .collect(),
                ))
                .build();

            self.client
                .complete_multipart_upload()
                .bucket(bucket)
                .key(key)
                .upload_id(&upload_id)
                .multipart_upload(multipart)
                .send()
                .await
                .map_err(|_| StatusCode::Failure)?;

            Ok::<(), StatusCode>(())
        }
        .await;

        if result.is_err() {
            let _ = self
                .client
                .abort_multipart_upload()
                .bucket(bucket)
                .key(key)
                .upload_id(&upload_id)
                .send()
                .await;
        }

        result
    }

    async fn remove_object(&self, bucket: &str, key: &str) -> Result<(), StatusCode> {
        self.client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|_| StatusCode::Failure)?;

        Ok(())
    }

    async fn rename_object(
        &self,
        old_bucket: &str,
        old_key: &str,
        new_bucket: &str,
        new_key: &str,
    ) -> Result<(), StatusCode> {
        let encoded_old_key = utf8_percent_encode(old_key, COPY_SOURCE_SET).to_string();
        let copy_source = format!("{old_bucket}/{encoded_old_key}");

        self.client
            .copy_object()
            .bucket(new_bucket)
            .key(new_key)
            .copy_source(copy_source)
            .send()
            .await
            .map_err(|_| StatusCode::Failure)?;

        self.client
            .delete_object()
            .bucket(old_bucket)
            .key(old_key)
            .send()
            .await
            .map_err(|_| StatusCode::Failure)?;

        Ok(())
    }

    async fn mkdir(&self, bucket: &str, key: &str) -> Result<(), StatusCode> {
        let marker_key = self.qualify_dir_prefix(key);

        self.client
            .put_object()
            .bucket(bucket)
            .key(marker_key)
            .body(ByteStream::from(Vec::<u8>::new()))
            .send()
            .await
            .map_err(|_| StatusCode::Failure)?;

        Ok(())
    }

    async fn rmdir(&self, bucket: &str, key: &str) -> Result<(), StatusCode> {
        let dir_prefix = self.qualify_dir_prefix(key);

        let resp = self
            .client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(dir_prefix.clone())
            .max_keys(2)
            .send()
            .await
            .map_err(|_| StatusCode::Failure)?;

        let keys = resp
            .contents()
            .iter()
            .filter_map(|item| item.key().map(ToOwned::to_owned))
            .collect::<Vec<_>>();

        if keys.len() > 1 || (keys.len() == 1 && keys[0] != dir_prefix) {
            return Err(StatusCode::Failure);
        }

        self.client
            .delete_object()
            .bucket(bucket)
            .key(dir_prefix)
            .send()
            .await
            .map_err(|_| StatusCode::Failure)?;

        Ok(())
    }
}

#[derive(Clone)]
struct SshServer {
    backend: Backend,
    username: String,
    password: String,
}

impl russh::server::Server for SshServer {
    type Handler = SshSession;

    fn new_client(&mut self, _peer_addr: Option<std::net::SocketAddr>) -> Self::Handler {
        SshSession {
            backend: self.backend.clone(),
            username: self.username.clone(),
            password: self.password.clone(),
            channels: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

struct SshSession {
    backend: Backend,
    username: String,
    password: String,
    channels: Arc<Mutex<HashMap<ChannelId, Channel<Msg>>>>,
}

impl SshSession {
    async fn take_channel(&self, channel_id: ChannelId) -> Option<Channel<Msg>> {
        let mut channels = self.channels.lock().await;
        channels.remove(&channel_id)
    }
}

impl russh::server::Handler for SshSession {
    type Error = anyhow::Error;

    async fn auth_password(&mut self, user: &str, password: &str) -> Result<Auth, Self::Error> {
        if user == self.username && password == self.password {
            Ok(Auth::Accept)
        } else {
            Ok(Auth::Reject {
                proceed_with_methods: None,
                partial_success: false,
            })
        }
    }

    async fn channel_open_session(
        &mut self,
        channel: Channel<Msg>,
        _session: &mut Session,
    ) -> Result<bool, Self::Error> {
        let mut channels = self.channels.lock().await;
        channels.insert(channel.id(), channel);
        Ok(true)
    }

    async fn channel_eof(
        &mut self,
        channel: ChannelId,
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        session.close(channel)?;
        Ok(())
    }

    async fn subsystem_request(
        &mut self,
        channel_id: ChannelId,
        name: &str,
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        if name != "sftp" {
            session.channel_failure(channel_id)?;
            return Ok(());
        }

        let Some(channel) = self.take_channel(channel_id).await else {
            session.channel_failure(channel_id)?;
            return Ok(());
        };

        session.channel_success(channel_id)?;
        let handler = SftpSession::new(self.backend.clone());

        tokio::spawn(async move {
            russh_sftp::server::run(channel.into_stream(), handler).await;
        });

        Ok(())
    }
}

enum HandleState {
    Read {
        bucket: String,
        key: String,
        size: u64,
        cache: Option<ReadCache>,
    },
    Write {
        bucket: String,
        key: String,
        temp_path: TempPath,
        file: TokioFile,
    },
    Dir {
        entries: Vec<File>,
        sent: bool,
    },
}

struct ReadCache {
    offset: u64,
    data: Vec<u8>,
}

struct SftpSession {
    backend: Backend,
    next_handle: u64,
    handles: HashMap<String, HandleState>,
}

impl SftpSession {
    fn new(backend: Backend) -> Self {
        Self {
            backend,
            next_handle: 0,
            handles: HashMap::new(),
        }
    }

    fn make_handle_id(&mut self) -> String {
        self.next_handle += 1;
        format!("h-{}", self.next_handle)
    }

    fn create_temp_file(&self) -> Result<(TempPath, TokioFile), StatusCode> {
        let named = TempFileBuilder::new()
            .prefix("sftp-rustfs-")
            .tempfile()
            .map_err(|_| StatusCode::Failure)?;

        let temp_path = named.into_temp_path();
        let std_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&temp_path)
            .map_err(|_| StatusCode::Failure)?;

        Ok((temp_path, TokioFile::from_std(std_file)))
    }

    async fn open_write_handle(
        &mut self,
        id: u32,
        bucket: String,
        key: String,
        pflags: OpenFlags,
    ) -> Result<Handle, StatusCode> {
        let handle_id = self.make_handle_id();
        let (temp_path, mut file) = self.create_temp_file()?;

        let should_seed_existing =
            !pflags.contains(OpenFlags::TRUNCATE) && !pflags.contains(OpenFlags::CREATE);

        if should_seed_existing {
            let _ = self
                .backend
                .seed_local_file_from_object(&bucket, &key, &mut file)
                .await;
        }

        if pflags.contains(OpenFlags::TRUNCATE) {
            file.set_len(0).await.map_err(|_| StatusCode::Failure)?;
            file.seek(SeekFrom::Start(0))
                .await
                .map_err(|_| StatusCode::Failure)?;
        }

        self.handles.insert(
            handle_id.clone(),
            HandleState::Write {
                bucket,
                key,
                temp_path,
                file,
            },
        );

        Ok(Handle {
            id,
            handle: handle_id,
        })
    }
}

impl russh_sftp::server::Handler for SftpSession {
    type Error = StatusCode;

    fn unimplemented(&self) -> Self::Error {
        StatusCode::OpUnsupported
    }

    async fn open(
        &mut self,
        id: u32,
        filename: String,
        pflags: OpenFlags,
        _attrs: FileAttributes,
    ) -> Result<Handle, Self::Error> {
        let path = Backend::split_path(&filename)?;

        let is_write = pflags.contains(OpenFlags::WRITE)
            || pflags.contains(OpenFlags::CREATE)
            || pflags.contains(OpenFlags::TRUNCATE)
            || pflags.contains(OpenFlags::APPEND);

        match path {
            SftpPath::Object { bucket, key } => {
                if !self.backend.bucket_exists(&bucket).await? {
                    return Err(StatusCode::NoSuchFile);
                }

                let qualified_key = self.backend.qualify_object_key(&key);

                if is_write {
                    self.open_write_handle(id, bucket, qualified_key, pflags).await
                } else {
                    let size = self.backend.head_object(&bucket, &qualified_key).await?;
                    let handle_id = self.make_handle_id();

                    self.handles.insert(
                        handle_id.clone(),
                        HandleState::Read {
                            bucket,
                            key: qualified_key,
                            size,
                            cache: None,
                        },
                    );

                    Ok(Handle {
                        id,
                        handle: handle_id,
                    })
                }
            }
            _ => Err(StatusCode::Failure),
        }
    }

    async fn close(&mut self, id: u32, handle: String) -> Result<Status, Self::Error> {
        let state = self.handles.remove(&handle).ok_or(StatusCode::NoSuchFile)?;

        if let HandleState::Write {
            bucket,
            key,
            temp_path,
            mut file,
        } = state
        {
            file.flush().await.map_err(|_| StatusCode::Failure)?;
            file.sync_all().await.map_err(|_| StatusCode::Failure)?;
            drop(file);

            let metadata = fs::metadata(&temp_path)
                .await
                .map_err(|_| StatusCode::Failure)?;

            if metadata.len() >= self.backend.tuning.multipart_threshold {
                self.backend
                    .multipart_upload_from_path(&bucket, &key, temp_path.as_ref(), metadata.len())
                    .await?;
            } else {
                self.backend
                    .put_object_from_path(&bucket, &key, temp_path.as_ref(), metadata.len())
                    .await?;
            }
        }

        Ok(Backend::ok_status(id))
    }

    async fn read(
        &mut self,
        id: u32,
        handle: String,
        offset: u64,
        len: u32,
    ) -> Result<Data, Self::Error> {
        let state = self.handles.get_mut(&handle).ok_or(StatusCode::NoSuchFile)?;

        match state {
            HandleState::Read {
                bucket,
                key,
                size,
                cache,
            } => {
                let requested_len = len.max(1) as usize;

                if let Some(cached) = cache.as_ref() {
                    let cache_end = cached.offset + cached.data.len() as u64;
                    let requested_end = offset.saturating_add(requested_len as u64);

                    if offset >= cached.offset && requested_end <= cache_end {
                        let start = (offset - cached.offset) as usize;
                        let end = start + requested_len;
                        return Ok(Data {
                            id,
                            data: cached.data[start..end].to_vec(),
                        });
                    }
                }

                let fetch_len = len.max(self.backend.tuning.read_ahead_chunk_size);
                let bytes = self
                    .backend
                    .read_object_range(bucket, key, offset, fetch_len, *size)
                    .await?;
                let response = bytes[..bytes.len().min(requested_len)].to_vec();

                *cache = Some(ReadCache {
                    offset,
                    data: bytes,
                });

                Ok(Data { id, data: response })
            }
            HandleState::Write { file, .. } => {
                file.seek(SeekFrom::Start(offset))
                    .await
                    .map_err(|_| StatusCode::Failure)?;

                let mut buffer = vec![0_u8; len.max(1) as usize];
                let read = file
                    .read(&mut buffer)
                    .await
                    .map_err(|_| StatusCode::Failure)?;

                if read == 0 {
                    return Err(StatusCode::Eof);
                }

                buffer.truncate(read);
                Ok(Data { id, data: buffer })
            }
            HandleState::Dir { .. } => Err(StatusCode::Failure),
        }
    }

    async fn write(
        &mut self,
        id: u32,
        handle: String,
        offset: u64,
        data: Vec<u8>,
    ) -> Result<Status, Self::Error> {
        let state = self.handles.get_mut(&handle).ok_or(StatusCode::NoSuchFile)?;
        let HandleState::Write { file, .. } = state else {
            return Err(StatusCode::PermissionDenied);
        };

        file.seek(SeekFrom::Start(offset))
            .await
            .map_err(|_| StatusCode::Failure)?;
        file.write_all(&data)
            .await
            .map_err(|_| StatusCode::Failure)?;

        Ok(Backend::ok_status(id))
    }

    async fn stat(&mut self, id: u32, path: String) -> Result<Attrs, Self::Error> {
        let attrs = self.backend.stat_path(&path).await?;
        Ok(Attrs { id, attrs })
    }

    async fn lstat(&mut self, id: u32, path: String) -> Result<Attrs, Self::Error> {
        let attrs = self.backend.stat_path(&path).await?;
        Ok(Attrs { id, attrs })
    }

    async fn fstat(&mut self, id: u32, handle: String) -> Result<Attrs, Self::Error> {
        let state = self.handles.get_mut(&handle).ok_or(StatusCode::NoSuchFile)?;

        let attrs = match state {
            HandleState::Read { size, .. } => Backend::file_attrs(*size),
            HandleState::Write { file, .. } => {
                let metadata = file.metadata().await.map_err(|_| StatusCode::Failure)?;
                Backend::file_attrs(metadata.len())
            }
            HandleState::Dir { .. } => Backend::dir_attrs(),
        };

        Ok(Attrs { id, attrs })
    }

    async fn opendir(&mut self, id: u32, path: String) -> Result<Handle, Self::Error> {
        let entries = self.backend.list_dir(&path).await?;
        let handle_id = self.make_handle_id();

        self.handles.insert(
            handle_id.clone(),
            HandleState::Dir {
                entries,
                sent: false,
            },
        );

        Ok(Handle {
            id,
            handle: handle_id,
        })
    }

    async fn readdir(&mut self, id: u32, handle: String) -> Result<Name, Self::Error> {
        let state = self.handles.get_mut(&handle).ok_or(StatusCode::NoSuchFile)?;
        let HandleState::Dir { entries, sent } = state else {
            return Err(StatusCode::Failure);
        };

        if *sent {
            return Err(StatusCode::Eof);
        }

        *sent = true;
        Ok(Name {
            id,
            files: entries.clone(),
        })
    }

    async fn remove(&mut self, id: u32, filename: String) -> Result<Status, Self::Error> {
        let path = Backend::split_path(&filename)?;
        let SftpPath::Object { bucket, key } = path else {
            return Err(StatusCode::Failure);
        };

        if !self.backend.bucket_exists(&bucket).await? {
            return Err(StatusCode::NoSuchFile);
        }

        let qualified_key = self.backend.qualify_object_key(&key);
        self.backend.remove_object(&bucket, &qualified_key).await?;
        Ok(Backend::ok_status(id))
    }

    async fn mkdir(
        &mut self,
        id: u32,
        path: String,
        _attrs: FileAttributes,
    ) -> Result<Status, Self::Error> {
        let path = Backend::split_path(&path)?;
        let SftpPath::Object { bucket, key } = path else {
            return Err(StatusCode::Failure);
        };

        if !self.backend.bucket_exists(&bucket).await? {
            return Err(StatusCode::NoSuchFile);
        }

        self.backend.mkdir(&bucket, &key).await?;
        Ok(Backend::ok_status(id))
    }

    async fn rmdir(&mut self, id: u32, path: String) -> Result<Status, Self::Error> {
        let path = Backend::split_path(&path)?;
        let SftpPath::Object { bucket, key } = path else {
            return Err(StatusCode::Failure);
        };

        if !self.backend.bucket_exists(&bucket).await? {
            return Err(StatusCode::NoSuchFile);
        }

        self.backend.rmdir(&bucket, &key).await?;
        Ok(Backend::ok_status(id))
    }

    async fn rename(
        &mut self,
        id: u32,
        oldpath: String,
        newpath: String,
    ) -> Result<Status, Self::Error> {
        let old_path = Backend::split_path(&oldpath)?;
        let new_path = Backend::split_path(&newpath)?;

        let SftpPath::Object {
            bucket: old_bucket,
            key: old_key,
        } = old_path
        else {
            return Err(StatusCode::Failure);
        };

        let SftpPath::Object {
            bucket: new_bucket,
            key: new_key,
        } = new_path
        else {
            return Err(StatusCode::Failure);
        };

        if !self.backend.bucket_exists(&old_bucket).await? {
            return Err(StatusCode::NoSuchFile);
        }
        if !self.backend.bucket_exists(&new_bucket).await? {
            return Err(StatusCode::NoSuchFile);
        }

        let old_key = self.backend.qualify_object_key(&old_key);
        let new_key = self.backend.qualify_object_key(&new_key);

        self.backend
            .rename_object(&old_bucket, &old_key, &new_bucket, &new_key)
            .await?;

        Ok(Backend::ok_status(id))
    }

    async fn realpath(&mut self, id: u32, path: String) -> Result<Name, Self::Error> {
        let normalized = Backend::normalize_path(&path)?;
        let full = if normalized.is_empty() {
            "/".to_string()
        } else {
            format!("/{normalized}")
        };

        Ok(Name {
            id,
            files: vec![File::dummy(full)],
        })
    }
}

fn load_or_create_host_key(path: &Path) -> Result<russh::keys::PrivateKey> {
    if path.exists() {
        return load_secret_key(path, None)
            .with_context(|| format!("failed to load host key from {}", path.display()));
    }

    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("failed to create host key directory {}", parent.display())
            })?;
        }
    }

    let key = russh::keys::PrivateKey::random(&mut OsRng, ssh_key::Algorithm::Ed25519)
        .map_err(|e| anyhow!("failed to generate host key: {e}"))?;

    key.write_openssh_file(path, LineEnding::LF)
        .with_context(|| format!("failed to persist host key to {}", path.display()))?;

    info!("Generated new host key at {}", path.display());
    Ok(key)
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let cfg = AppConfig::from_env()?;
    let backend = Backend::new(&cfg).await?;
    let host_key = load_or_create_host_key(&cfg.host_key_path)?;

    let ssh_config = russh::server::Config {
        inactivity_timeout: Some(Duration::from_secs(3600)),
        auth_rejection_time: Duration::from_secs(3),
        auth_rejection_time_initial: Some(Duration::from_secs(0)),
        window_size: SSH_WINDOW_SIZE,
        maximum_packet_size: SSH_MAX_PACKET_SIZE,
        nodelay: true,
        keys: vec![host_key],
        ..Default::default()
    };

    info!("SFTP bind: {}", cfg.bind_addr);
    info!("Host key path: {}", cfg.host_key_path.display());
    info!("RustFS endpoint: {}", cfg.rustfs_endpoint);
    info!(
        "Visible buckets: {}",
        cfg.visible_buckets
            .as_ref()
            .map(|items| items.join(","))
            .unwrap_or_else(|| "*".to_string())
    );
    info!(
        "Default prefix inside each bucket: {}",
        if cfg.default_prefix.is_empty() {
            "/"
        } else {
            &cfg.default_prefix
        }
    );
    info!(
        "Transfer tuning: read_ahead={} multipart_threshold={} multipart_chunk={} multipart_concurrency={} stream_buffer={}",
        cfg.transfer_tuning.read_ahead_chunk_size,
        cfg.transfer_tuning.multipart_threshold,
        cfg.transfer_tuning.multipart_chunk_size,
        cfg.transfer_tuning.multipart_upload_concurrency,
        cfg.transfer_tuning.stream_buffer_size
    );

    let mut server = SshServer {
        backend,
        username: cfg.sftp_user,
        password: cfg.sftp_password,
    };

    if let Err(err) = server.run_on_address(Arc::new(ssh_config), cfg.bind_addr).await {
        error!("server stopped with error: {err:#}");
        return Err(err.into());
    }

    Ok(())
}