use crate::command::{integer, Condition, Expiry, Operation};
use crate::expiry::ExpiryIndex;
use bytes::Bytes;
use recall_protocol::Reply;
use std::collections::HashMap;
use std::fmt;

#[derive(Clone, Debug)]
pub struct ShardLimits {
    pub max_keys: usize,
    pub max_payload_bytes: usize,
    pub max_reply_bytes: usize,
}

impl Default for ShardLimits {
    fn default() -> Self {
        Self {
            max_keys: 16_384,
            max_payload_bytes: 32 * 1024 * 1024,
            max_reply_bytes: 4 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EngineError(pub String);

impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for EngineError {}

impl EngineError {
    pub fn reply(self) -> Reply {
        Reply::error(self.0)
    }
}

#[derive(Clone)]
struct Entry {
    value: Bytes,
    expires_at_ms: Option<i64>,
}

/// Valid only while this owner remains at the revision that prepared it.
/// Dropping a preparation has no effect on the committed table.
pub struct Prepared {
    revision: u64,
    effects: HashMap<Bytes, Option<Entry>>,
    payload_after: usize,
    response: Reply,
}

impl Prepared {
    pub fn response(&self) -> &Reply {
        &self.response
    }
}

#[derive(Clone, Debug, Default)]
pub struct ShardStats {
    pub keys: usize,
    pub payload_bytes: usize,
    pub expiring_keys: usize,
    pub expired_keys: u64,
    pub applied_commands: u64,
    pub max_keys: usize,
    pub max_payload_bytes: usize,
}

pub struct Shard {
    entries: HashMap<Bytes, Entry>,
    expiry: ExpiryIndex,
    limits: ShardLimits,
    payload_bytes: usize,
    revision: u64,
    expired_keys: u64,
    applied_commands: u64,
}

impl Shard {
    pub fn new(limits: ShardLimits) -> Result<Self, EngineError> {
        if limits.max_keys == 0 || limits.max_payload_bytes == 0 || limits.max_reply_bytes < 64 {
            return Err(failure("ERR invalid shard limits"));
        }
        let mut entries = HashMap::new();
        entries
            .try_reserve(limits.max_keys)
            .map_err(|_| failure("OOM could not reserve key table"))?;
        let expiry = ExpiryIndex::new(limits.max_keys)
            .map_err(|_| failure("OOM could not reserve expiration index"))?;
        Ok(Self {
            entries,
            expiry,
            limits,
            payload_bytes: 0,
            revision: 0,
            expired_keys: 0,
            applied_commands: 0,
        })
    }

    pub fn stats(&self) -> ShardStats {
        ShardStats {
            keys: self.entries.len(),
            payload_bytes: self.payload_bytes,
            expiring_keys: self.expiry.len(),
            expired_keys: self.expired_keys,
            applied_commands: self.applied_commands,
            max_keys: self.limits.max_keys,
            max_payload_bytes: self.limits.max_payload_bytes,
        }
    }

    pub fn execute(&mut self, operation: &Operation, now_ms: i64) -> Result<Reply, EngineError> {
        let prepared = self.prepare(operation, now_ms)?;
        Ok(self.apply(prepared))
    }

    pub fn prepare(&self, operation: &Operation, now_ms: i64) -> Result<Prepared, EngineError> {
        let mut effects = HashMap::new();
        effects
            .try_reserve(operation.keys().len())
            .map_err(|_| failure("OOM could not reserve command effects"))?;
        let response = match operation {
            Operation::Get(key) => Reply::Bulk(
                self.observe(key, now_ms, &mut effects)
                    .map(|entry| entry.value.clone()),
            ),
            Operation::Set {
                key,
                value,
                condition,
                expiry,
            } => {
                let old = self.observe(key, now_ms, &mut effects);
                let deadline = match expiry {
                    Expiry::Clear => None,
                    Expiry::Keep => old.and_then(|entry| entry.expires_at_ms),
                    Expiry::AfterMilliseconds(duration) => Some(
                        now_ms
                            .checked_add(*duration)
                            .filter(|_| *duration > 0)
                            .ok_or_else(|| failure("ERR invalid expire time in 'set' command"))?,
                    ),
                };
                if (*condition == Condition::IfAbsent && old.is_some())
                    || (*condition == Condition::IfPresent && old.is_none())
                {
                    Reply::Bulk(None)
                } else {
                    effects.insert(
                        key.clone(),
                        Some(Entry {
                            value: value.clone(),
                            expires_at_ms: deadline,
                        }),
                    );
                    Reply::ok()
                }
            }
            Operation::Increment {
                key,
                amount,
                subtract,
            } => {
                // DECRBY's minimum argument cannot be represented as a
                // positive signed increment; reject it without mutation.
                if *subtract && *amount == i64::MIN {
                    return Err(failure("ERR increment or decrement would overflow"));
                }
                let old = self.observe(key, now_ms, &mut effects);
                let current = old
                    .map(|entry| integer(&entry.value))
                    .transpose()
                    .map_err(|error| EngineError(error.0))?
                    .unwrap_or(0);
                let next = if *subtract {
                    current.checked_sub(*amount)
                } else {
                    current.checked_add(*amount)
                }
                .ok_or_else(|| failure("ERR increment or decrement would overflow"))?;
                let expires_at_ms = old.and_then(|entry| entry.expires_at_ms);
                effects.insert(
                    key.clone(),
                    Some(Entry {
                        value: Bytes::from(next.to_string()),
                        expires_at_ms,
                    }),
                );
                Reply::Integer(next)
            }
            Operation::Delete(keys) => {
                let mut removed = 0;
                for key in keys {
                    if effects.contains_key(key) {
                        continue;
                    }
                    if self.observe(key, now_ms, &mut effects).is_some() {
                        removed += 1;
                    }
                    effects.insert(key.clone(), None);
                }
                Reply::Integer(removed)
            }
            Operation::Exists(keys) => Reply::Integer(
                keys.iter()
                    .filter(|key| self.observe(key, now_ms, &mut effects).is_some())
                    .count() as i64,
            ),
            Operation::MultiGet(keys) => Reply::Array(
                keys.iter()
                    .map(|key| {
                        Reply::Bulk(
                            self.observe(key, now_ms, &mut effects)
                                .map(|entry| entry.value.clone()),
                        )
                    })
                    .collect(),
            ),
            Operation::MultiSet(pairs) => {
                for (key, value) in pairs {
                    effects.insert(
                        key.clone(),
                        Some(Entry {
                            value: value.clone(),
                            expires_at_ms: None,
                        }),
                    );
                }
                Reply::ok()
            }
            Operation::Expire { key, milliseconds } => {
                if let Some(old) = self.observe(key, now_ms, &mut effects) {
                    if *milliseconds <= 0 {
                        effects.insert(key.clone(), None);
                    } else {
                        let deadline = now_ms.checked_add(*milliseconds).ok_or_else(|| {
                            failure("ERR invalid expire time in 'expire' command")
                        })?;
                        let mut entry = old.clone();
                        entry.expires_at_ms = Some(deadline);
                        effects.insert(key.clone(), Some(entry));
                    }
                    Reply::Integer(1)
                } else {
                    Reply::Integer(0)
                }
            }
            Operation::Ttl { key, milliseconds } => {
                let ttl = match self.observe(key, now_ms, &mut effects) {
                    None => -2,
                    Some(Entry {
                        expires_at_ms: None,
                        ..
                    }) => -1,
                    Some(Entry {
                        expires_at_ms: Some(deadline),
                        ..
                    }) => {
                        let remaining = deadline.saturating_sub(now_ms);
                        if *milliseconds {
                            remaining
                        } else {
                            remaining / 1000 + i64::from(remaining % 1000 >= 500)
                        }
                    }
                };
                Reply::Integer(ttl)
            }
            Operation::Persist(key) => {
                if let Some(old) = self.observe(key, now_ms, &mut effects) {
                    if old.expires_at_ms.is_some() {
                        let mut entry = old.clone();
                        entry.expires_at_ms = None;
                        effects.insert(key.clone(), Some(entry));
                        Reply::Integer(1)
                    } else {
                        Reply::Integer(0)
                    }
                } else {
                    Reply::Integer(0)
                }
            }
        };
        self.preflight(effects, response)
    }

    /// The caller must exclude all other applications and expiration work
    /// between prepare and apply. Cross-owner transactions retain reservations.
    pub fn apply(&mut self, prepared: Prepared) -> Reply {
        assert_eq!(self.revision, prepared.revision, "stale prepared command");
        for (key, entry) in prepared.effects {
            if let Some(entry) = entry {
                if let Some(deadline) = entry.expires_at_ms {
                    self.expiry.set(key.clone(), deadline);
                } else {
                    self.expiry.remove(&key);
                }
                self.entries.insert(key, entry);
            } else {
                self.expiry.remove(&key);
                self.entries.remove(&key);
            }
        }
        self.payload_bytes = prepared.payload_after;
        self.revision = self
            .revision
            .checked_add(1)
            .expect("owner revision exhausted");
        self.applied_commands = self.applied_commands.saturating_add(1);
        prepared.response
    }

    /// Work is bounded by the number of removed keys, not the entire table size.
    /// Persistent modes must replace this memory-only removal with logged effects.
    pub fn expire_due(&mut self, now_ms: i64, budget: usize) -> usize {
        let mut removed = 0;
        while removed < budget {
            let Some(key) = self.expiry.expired(now_ms) else {
                break;
            };
            self.expiry.remove(&key);
            let entry = self
                .entries
                .remove(&key)
                .expect("timer without live table entry");
            assert!(entry
                .expires_at_ms
                .is_some_and(|deadline| deadline <= now_ms));
            self.payload_bytes -= key.len() + entry.value.len();
            self.expired_keys = self.expired_keys.saturating_add(1);
            self.revision = self
                .revision
                .checked_add(1)
                .expect("owner revision exhausted");
            removed += 1;
        }
        removed
    }

    fn observe<'a>(
        &'a self,
        key: &Bytes,
        now_ms: i64,
        effects: &mut HashMap<Bytes, Option<Entry>>,
    ) -> Option<&'a Entry> {
        let entry = self.entries.get(key)?;
        if entry
            .expires_at_ms
            .is_some_and(|deadline| deadline <= now_ms)
        {
            effects.insert(key.clone(), None);
            None
        } else {
            Some(entry)
        }
    }

    fn preflight(
        &self,
        effects: HashMap<Bytes, Option<Entry>>,
        response: Reply,
    ) -> Result<Prepared, EngineError> {
        if response
            .encoded_len()
            .is_none_or(|bytes| bytes > self.limits.max_reply_bytes)
        {
            return Err(failure("ERR response exceeds configured limit"));
        }
        let mut keys = self.entries.len() as i128;
        let mut payload = self.payload_bytes as i128;
        for (key, next) in &effects {
            if let Some(old) = self.entries.get(key) {
                keys -= 1;
                payload -= (key.len() + old.value.len()) as i128;
            }
            if let Some(next) = next {
                keys += 1;
                payload += (key.len() + next.value.len()) as i128;
            }
        }
        if keys > self.limits.max_keys as i128 || payload > self.limits.max_payload_bytes as i128 {
            return Err(failure("OOM owner key or payload budget exhausted"));
        }
        let payload_after = usize::try_from(payload).expect("negative payload accounting");
        Ok(Prepared {
            revision: self.revision,
            effects,
            payload_after,
            response,
        })
    }
}

fn failure(message: &str) -> EngineError {
    EngineError(message.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{parse, Command, ParseLimits};

    fn run(shard: &mut Shard, args: &[&str], now: i64) -> Reply {
        let args: Vec<_> = args
            .iter()
            .map(|s| Bytes::copy_from_slice(s.as_bytes()))
            .collect();
        let Command::Data(operation) = parse(&args, &ParseLimits::default()).unwrap() else {
            panic!("expected data command")
        };
        shard
            .execute(&operation, now)
            .unwrap_or_else(EngineError::reply)
    }

    fn shard() -> Shard {
        Shard::new(ShardLimits {
            max_keys: 100,
            ..ShardLimits::default()
        })
        .unwrap()
    }

    #[test]
    fn assignment_conditions_and_deadlines_are_atomic() {
        let mut shard = shard();
        assert_eq!(
            run(&mut shard, &["SET", "k", "1", "NX", "PX", "1000"], 0),
            Reply::ok()
        );
        assert_eq!(
            run(&mut shard, &["SET", "k", "2", "NX"], 1),
            Reply::Bulk(None)
        );
        assert_eq!(
            run(&mut shard, &["SET", "k", "2", "XX", "KEEPTTL"], 10),
            Reply::ok()
        );
        assert_eq!(run(&mut shard, &["PTTL", "k"], 11), Reply::Integer(989));
        assert_eq!(run(&mut shard, &["GET", "k"], 999), Reply::bulk("2"));
        assert_eq!(run(&mut shard, &["GET", "k"], 1000), Reply::Bulk(None));
        assert_eq!(run(&mut shard, &["GET", "k"], 0), Reply::Bulk(None));
    }

    #[test]
    fn counters_check_overflow_without_mutation_and_keep_expiry() {
        let mut shard = shard();
        run(
            &mut shard,
            &["SET", "k", "9223372036854775807", "PX", "10000"],
            0,
        );
        assert!(matches!(
            run(&mut shard, &["INCR", "k"], 1),
            Reply::Error(_)
        ));
        assert_eq!(
            run(&mut shard, &["GET", "k"], 2),
            Reply::bulk("9223372036854775807")
        );
        assert_eq!(
            run(&mut shard, &["DECR", "k"], 2),
            Reply::Integer(i64::MAX - 1)
        );
        assert_eq!(run(&mut shard, &["PTTL", "k"], 2), Reply::Integer(9998));
        run(&mut shard, &["SET", "k", "-9223372036854775808"], 3);
        assert!(matches!(
            run(&mut shard, &["DECRBY", "k", "-9223372036854775808"], 3),
            Reply::Error(_)
        ));
        assert_eq!(
            run(&mut shard, &["GET", "k"], 3),
            Reply::bulk("-9223372036854775808")
        );
    }

    #[test]
    fn duplicate_multi_key_semantics_match_command_intent() {
        let mut shard = shard();
        assert_eq!(
            run(&mut shard, &["MSET", "a", "1", "b", "2", "a", "3"], 0),
            Reply::ok()
        );
        assert_eq!(
            run(&mut shard, &["MGET", "a", "b", "a", "z"], 0),
            Reply::Array(vec![
                Reply::bulk("3"),
                Reply::bulk("2"),
                Reply::bulk("3"),
                Reply::Bulk(None),
            ])
        );
        assert_eq!(
            run(&mut shard, &["EXISTS", "a", "a", "b"], 0),
            Reply::Integer(3)
        );
        assert_eq!(
            run(&mut shard, &["DEL", "a", "a", "b"], 0),
            Reply::Integer(2)
        );
        assert_eq!(shard.stats().payload_bytes, 0);
    }

    #[test]
    fn failed_multi_assignment_has_no_partial_effects() {
        let mut shard = Shard::new(ShardLimits {
            max_keys: 2,
            max_payload_bytes: 8,
            max_reply_bytes: 1024,
        })
        .unwrap();
        run(&mut shard, &["SET", "a", "old"], 0);
        assert!(matches!(
            run(&mut shard, &["MSET", "a", "new", "b", "toolarge"], 0),
            Reply::Error(_)
        ));
        assert_eq!(run(&mut shard, &["GET", "a"], 0), Reply::bulk("old"));
        assert_eq!(run(&mut shard, &["GET", "b"], 0), Reply::Bulk(None));
        assert_eq!(shard.stats().payload_bytes, 4);
    }

    #[test]
    fn dropping_preparation_rolls_back_every_requested_change() {
        let mut shard = shard();
        let operation =
            Operation::MultiSet(vec![(Bytes::from_static(b"a"), Bytes::from_static(b"1"))]);
        let prepared = shard.prepare(&operation, 0).unwrap();
        assert_eq!(prepared.response(), &Reply::ok());
        drop(prepared);
        assert_eq!(run(&mut shard, &["GET", "a"], 0), Reply::Bulk(None));
    }

    #[test]
    fn expiry_updates_do_not_leak_timers_and_cleanup_is_bounded() {
        let mut shard = shard();
        for key in ["a", "b", "c"] {
            run(&mut shard, &["SET", key, "1", "PX", "10"], 0);
        }
        for _ in 0..1000 {
            run(&mut shard, &["PEXPIRE", "a", "10"], 0);
        }
        assert_eq!(shard.stats().expiring_keys, 3);
        assert_eq!(shard.expire_due(10, 2), 2);
        assert_eq!(shard.stats().keys, 1);
        assert_eq!(shard.expire_due(10, 2), 1);
        assert_eq!(shard.stats().payload_bytes, 0);
    }

    #[test]
    fn plain_assignment_clears_ttl_and_nonpositive_expiry_deletes() {
        let mut shard = shard();
        run(&mut shard, &["SET", "k", "1", "PX", "1500"], 0);
        assert_eq!(run(&mut shard, &["TTL", "k"], 0), Reply::Integer(2));
        assert_eq!(run(&mut shard, &["PTTL", "k"], 0), Reply::Integer(1500));
        run(&mut shard, &["SET", "k", "2"], 1);
        assert_eq!(run(&mut shard, &["PTTL", "k"], 1), Reply::Integer(-1));
        assert_eq!(run(&mut shard, &["EXPIRE", "k", "0"], 1), Reply::Integer(1));
        assert_eq!(run(&mut shard, &["PTTL", "k"], 1), Reply::Integer(-2));
    }

    #[test]
    fn bounded_random_counter_sequence_matches_scalar_oracle() {
        let mut shard = shard();
        let mut seed = 7_u64;
        let mut expected = 0_i64;
        for _ in 0..10_000 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let amount = ((seed >> 32) % 201) as i64 - 100;
            expected += amount;
            assert_eq!(
                run(&mut shard, &["INCRBY", "n", &amount.to_string()], 0),
                Reply::Integer(expected)
            );
        }
    }

    #[test]
    fn shard_limits_are_validated_at_construction() {
        assert!(Shard::new(ShardLimits {
            max_keys: 0,
            ..ShardLimits::default()
        })
        .is_err());
        assert!(Shard::new(ShardLimits {
            max_keys: 1,
            max_payload_bytes: 0,
            ..ShardLimits::default()
        })
        .is_err());
        assert!(Shard::new(ShardLimits {
            max_keys: 1,
            max_reply_bytes: 63,
            ..ShardLimits::default()
        })
        .is_err());
        assert!(Shard::new(ShardLimits {
            max_keys: 1,
            max_payload_bytes: 1,
            max_reply_bytes: 64,
        })
        .is_ok());
    }

    #[test]
    fn key_capacity_rejects_new_keys_but_allows_replacement() {
        let mut shard = Shard::new(ShardLimits {
            max_keys: 1,
            max_payload_bytes: 1024,
            max_reply_bytes: 1024,
        })
        .unwrap();
        assert_eq!(run(&mut shard, &["SET", "a", "1"], 0), Reply::ok());
        assert_eq!(
            run(&mut shard, &["SET", "b", "2"], 0),
            Reply::error("OOM owner key or payload budget exhausted")
        );
        // Replacing an existing key stays within the key budget.
        assert_eq!(run(&mut shard, &["SET", "a", "2"], 0), Reply::ok());
        assert_eq!(run(&mut shard, &["GET", "a"], 0), Reply::bulk("2"));
        assert_eq!(shard.stats().keys, 1);
    }

    #[test]
    fn oversized_replies_are_rejected_without_touching_state() {
        let value = "x".repeat(64);
        let mut shard = Shard::new(ShardLimits {
            max_keys: 10,
            max_payload_bytes: 1024,
            max_reply_bytes: 64,
        })
        .unwrap();
        assert_eq!(run(&mut shard, &["SET", "k", value.as_str()], 0), Reply::ok());
        // A 64-byte bulk payload encodes past the 64-byte reply budget.
        assert_eq!(
            run(&mut shard, &["GET", "k"], 0),
            Reply::error("ERR response exceeds configured limit")
        );
        // The failed read neither deleted nor mutated the entry.
        assert_eq!(run(&mut shard, &["EXISTS", "k"], 0), Reply::Integer(1));
    }

    #[test]
    fn failed_conditions_preserve_the_existing_value_and_expiry() {
        let mut shard = shard();
        run(&mut shard, &["SET", "k", "1", "PX", "1000"], 0);
        // XX on a missing key stores nothing.
        assert_eq!(
            run(&mut shard, &["SET", "missing", "v", "XX"], 0),
            Reply::Bulk(None)
        );
        assert_eq!(run(&mut shard, &["EXISTS", "missing"], 0), Reply::Integer(0));
        // A failed NX leaves the old value and its deadline untouched.
        assert_eq!(run(&mut shard, &["SET", "k", "2", "NX"], 1), Reply::Bulk(None));
        assert_eq!(run(&mut shard, &["GET", "k"], 2), Reply::bulk("1"));
        assert_eq!(run(&mut shard, &["PTTL", "k"], 2), Reply::Integer(998));
        // KEEPTTL on a missing key has no deadline to keep.
        assert_eq!(run(&mut shard, &["SET", "fresh", "v", "KEEPTTL"], 2), Reply::ok());
        assert_eq!(run(&mut shard, &["PTTL", "fresh"], 2), Reply::Integer(-1));
        // MSET clears any existing deadline.
        assert_eq!(run(&mut shard, &["MSET", "k", "3"], 3), Reply::ok());
        assert_eq!(run(&mut shard, &["PTTL", "k"], 3), Reply::Integer(-1));
    }

    #[test]
    fn ttl_rounding_and_persist_boundaries() {
        let mut shard = shard();
        run(&mut shard, &["SET", "a", "1", "PX", "1499"], 0);
        assert_eq!(run(&mut shard, &["TTL", "a"], 0), Reply::Integer(1));
        run(&mut shard, &["SET", "b", "1", "PX", "1500"], 0);
        assert_eq!(run(&mut shard, &["TTL", "b"], 0), Reply::Integer(2));
        run(&mut shard, &["SET", "c", "1", "PX", "500"], 0);
        assert_eq!(run(&mut shard, &["TTL", "c"], 0), Reply::Integer(1));
        run(&mut shard, &["SET", "d", "1", "PX", "499"], 0);
        assert_eq!(run(&mut shard, &["TTL", "d"], 0), Reply::Integer(0));
        // PERSIST: missing and non-expiring keys return 0; expiring keys lose
        // their deadline and return 1.
        assert_eq!(run(&mut shard, &["PERSIST", "missing"], 0), Reply::Integer(0));
        run(&mut shard, &["SET", "plain", "1"], 0);
        assert_eq!(run(&mut shard, &["PERSIST", "plain"], 0), Reply::Integer(0));
        assert_eq!(run(&mut shard, &["PERSIST", "a"], 0), Reply::Integer(1));
        assert_eq!(run(&mut shard, &["PTTL", "a"], 0), Reply::Integer(-1));
        // PEXPIRE on a missing key is a no-op.
        assert_eq!(run(&mut shard, &["PEXPIRE", "missing", "1000"], 0), Reply::Integer(0));
    }

    #[test]
    fn deadline_overflow_is_rejected_without_mutation() {
        let mut shard = shard();
        run(&mut shard, &["SET", "k", "1"], 0);
        assert_eq!(
            run(&mut shard, &["PEXPIRE", "k", "9223372036854775807"], 1),
            Reply::error("ERR invalid expire time in 'expire' command")
        );
        assert_eq!(run(&mut shard, &["PTTL", "k"], 1), Reply::Integer(-1));
        assert_eq!(
            run(&mut shard, &["SET", "k", "2", "PX", "9223372036854775807"], 1),
            Reply::error("ERR invalid expire time in 'set' command")
        );
        assert_eq!(run(&mut shard, &["GET", "k"], 1), Reply::bulk("1"));
    }

    #[test]
    fn expiry_accounting_distinguishes_lazy_and_scheduled_removal() {
        let mut shard = shard();
        run(&mut shard, &["SET", "lazy", "1", "PX", "10"], 0);
        run(&mut shard, &["SET", "due", "1", "PX", "10"], 0);
        // A read past the deadline removes the key without touching expired_keys.
        assert_eq!(run(&mut shard, &["GET", "lazy"], 10), Reply::Bulk(None));
        let stats = shard.stats();
        assert_eq!(stats.keys, 1);
        assert_eq!(stats.expired_keys, 0);
        // Scheduled reclamation is counted and bounded by its budget.
        assert_eq!(shard.expire_due(9, 10), 0);
        assert_eq!(shard.expire_due(10, 0), 0);
        assert_eq!(shard.expire_due(10, 10), 1);
        let stats = shard.stats();
        assert_eq!(stats.keys, 0);
        assert_eq!(stats.expired_keys, 1);
        assert_eq!(stats.expiring_keys, 0);
    }

    #[test]
    #[should_panic(expected = "stale prepared command")]
    fn applying_a_stale_preparation_panics() {
        let mut shard = shard();
        let operation = Operation::Get(Bytes::from_static(b"k"));
        let prepared = shard.prepare(&operation, 0).unwrap();
        // Any other application advances the revision.
        run(&mut shard, &["SET", "k", "1"], 0);
        let _ = shard.apply(prepared);
    }
}
