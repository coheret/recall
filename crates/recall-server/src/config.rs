use bytes::Bytes;
use recall_core::{ParseLimits, ShardLimits};
use recall_protocol::Limits;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

/// Passwords are intentionally not included in a derived Debug representation.
#[derive(Clone)]
pub struct Config {
    pub bind: SocketAddr,
    pub workers: usize,
    pub io_threads: usize,
    pub max_connections: usize,
    pub queue_capacity: usize,
    pub queue_bytes: usize,
    pub admission_timeout: Duration,
    pub read_timeout: Duration,
    pub write_timeout: Duration,
    pub expiry_interval: Duration,
    pub expiry_batch: usize,
    pub max_payload_bytes: usize,
    pub keys_per_worker: usize,
    pub max_reply_bytes: usize,
    pub protocol: Limits,
    pub commands: ParseLimits,
    pub password: Option<Bytes>,
}

impl Default for Config {
    fn default() -> Self {
        let cpus = std::thread::available_parallelism().map_or(4, |count| count.get());
        Self {
            bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6379),
            workers: cpus.saturating_sub(2).clamp(1, 16),
            io_threads: 2,
            max_connections: 128,
            queue_capacity: 256,
            queue_bytes: 4 * 1024 * 1024,
            admission_timeout: Duration::from_millis(250),
            read_timeout: Duration::from_secs(30),
            write_timeout: Duration::from_secs(10),
            expiry_interval: Duration::from_millis(10),
            expiry_batch: 64,
            max_payload_bytes: 512 * 1024 * 1024,
            keys_per_worker: 16_384,
            max_reply_bytes: 4 * 1024 * 1024,
            protocol: Limits::default(),
            commands: ParseLimits::default(),
            password: None,
        }
    }
}

impl Config {
    pub fn validate(&self) -> io::Result<()> {
        if !self.bind.ip().is_loopback() {
            return Err(invalid(
                "this milestone permits loopback binds only; transport security is not implemented",
            ));
        }
        if !(1..=64).contains(&self.workers)
            || !(1..=64).contains(&self.io_threads)
            || !(1..=65_536).contains(&self.max_connections)
            || !(1..=65_536).contains(&self.queue_capacity)
            || self.queue_bytes == 0
            || self.queue_bytes > u32::MAX as usize
            || self.keys_per_worker == 0
            || self.keys_per_worker > 1_048_576
            || self.max_payload_bytes < self.workers
        {
            return Err(invalid(
                "invalid worker, connection, queue, key, or payload limit",
            ));
        }
        if self.admission_timeout.is_zero()
            || self.read_timeout.is_zero()
            || self.write_timeout.is_zero()
            || self.expiry_interval.is_zero()
            || self.expiry_batch == 0
        {
            return Err(invalid("timeouts and expiration budgets must be nonzero"));
        }
        self.protocol
            .validate()
            .map_err(|error| invalid(&error.to_string()))?;
        if self.protocol.max_frame_bytes > 16 * 1024 * 1024
            || self.protocol.max_arguments > 4096
            || self.commands.max_key_bytes > self.protocol.max_bulk_bytes
            || self.commands.max_value_bytes > self.protocol.max_bulk_bytes
            || self.commands.max_client_metadata_bytes > 1024
            || self.max_reply_bytes < self.commands.max_value_bytes.saturating_add(64)
            || self.max_reply_bytes > 64 * 1024 * 1024
        {
            return Err(invalid(
                "inconsistent or excessive protocol/command/reply limits",
            ));
        }
        if self
            .password
            .as_ref()
            .is_some_and(|password| password.is_empty() || password.len() > 1024)
        {
            return Err(invalid(
                "the configured password must contain 1 to 1024 bytes",
            ));
        }
        Ok(())
    }

    pub fn shard_limits(&self, owner: usize) -> ShardLimits {
        ShardLimits {
            max_keys: self.keys_per_worker,
            max_payload_bytes: self.max_payload_bytes / self.workers
                + usize::from(owner < self.max_payload_bytes % self.workers),
            max_reply_bytes: self.max_reply_bytes,
        }
    }
}
pub(crate) fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_public_binds_even_with_a_password() {
        let config = Config {
            bind: "0.0.0.0:6379".parse().unwrap(),
            password: Some(Bytes::from_static(b"secret")),
            ..Config::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_empty_and_oversized_passwords() {
        let validate = |password: &[u8]| {
            Config {
                password: Some(Bytes::copy_from_slice(password)),
                ..Config::default()
            }
            .validate()
        };
        // Invalid configured credentials fail startup; they never silently
        // disable authentication or start with a weakened credential.
        assert!(validate(b"").is_err());
        assert!(validate(b"x").is_ok());
        assert!(validate(&[b'x'; 1024]).is_ok());
        assert!(validate(&[b'x'; 1025]).is_err());
    }

    #[test]
    fn apportions_payload_budget_without_losing_remainder() {
        let config = Config {
            workers: 3,
            max_payload_bytes: 100,
            ..Config::default()
        };
        assert_eq!(
            (0..3)
                .map(|owner| config.shard_limits(owner).max_payload_bytes)
                .sum::<usize>(),
            100
        );
    }
}
