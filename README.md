# sftp-rustfs-facade

SFTP facade over an S3-compatible RustFS backend.

## Required environment variables

The service will not start unless these variables are set and non-empty:

- `SFTP_PASSWORD`: password for the SFTP user.
- `RUSTFS_ACCESS_KEY`: access key for the RustFS/S3 endpoint.
- `RUSTFS_SECRET_KEY`: secret key for the RustFS/S3 endpoint.

## Common optional environment variables

- `SFTP_BIND`: listen address, default `0.0.0.0:2222`.
- `SFTP_USER`: SFTP login, default `sftp`.
- `RUSTFS_ENDPOINT`: S3-compatible endpoint, default `http://127.0.0.1:9000`.
- `RUSTFS_REGION`: region, default `us-east-1`.
- `RUSTFS_VISIBLE_BUCKETS`: comma-separated bucket allowlist.
- `RUSTFS_PREFIX`: prefix exposed inside each bucket.
- `SFTP_HOST_KEY`: path to the persisted SSH host key. If unset, the service uses `.sftp-rustfs-host-key` in the project directory.

See `.env.example` for a complete example, including optional transfer-tuning variables.

## Quick start

1. Copy `.env.example` to `.env` and fill in real secrets.
2. Export the variables in your shell or load them through your process manager.
3. Start the server:

```bash
cargo run
```

On first start, the server will create the SSH host key file if it does not exist yet.