//! The implemented command surface and RESP2 discovery metadata.

use bytes::Bytes;
use recall_protocol::Reply;
use std::fmt;

#[derive(Clone, Debug)]
pub struct ParseLimits {
    pub max_key_bytes: usize,
    pub max_value_bytes: usize,
    pub max_client_metadata_bytes: usize,
}

impl Default for ParseLimits {
    fn default() -> Self {
        Self {
            max_key_bytes: 1024,
            max_value_bytes: 512 * 1024,
            max_client_metadata_bytes: 128,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandError(pub String);

impl fmt::Display for CommandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for CommandError {}

impl CommandError {
    pub fn reply(self) -> Reply {
        Reply::error(self.0)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Condition {
    Always,
    IfAbsent,
    IfPresent,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Expiry {
    Clear,
    Keep,
    AfterMilliseconds(i64),
}

#[derive(Clone, Debug)]
pub enum Operation {
    Get(Bytes),
    Set {
        key: Bytes,
        value: Bytes,
        condition: Condition,
        expiry: Expiry,
    },
    Increment {
        key: Bytes,
        amount: i64,
        subtract: bool,
    },
    Delete(Vec<Bytes>),
    Exists(Vec<Bytes>),
    MultiGet(Vec<Bytes>),
    MultiSet(Vec<(Bytes, Bytes)>),
    Expire {
        key: Bytes,
        milliseconds: i64,
    },
    Ttl {
        key: Bytes,
        milliseconds: bool,
    },
    Persist(Bytes),
}

impl Operation {
    pub fn keys(&self) -> Vec<&Bytes> {
        match self {
            Self::Get(key)
            | Self::Persist(key)
            | Self::Set { key, .. }
            | Self::Increment { key, .. }
            | Self::Expire { key, .. }
            | Self::Ttl { key, .. } => vec![key],
            Self::Delete(keys) | Self::Exists(keys) | Self::MultiGet(keys) => keys.iter().collect(),
            Self::MultiSet(pairs) => pairs.iter().map(|(key, _)| key).collect(),
        }
    }

    pub fn payload_bytes(&self) -> usize {
        match self {
            Self::Set { key, value, .. } => key.len() + value.len(),
            Self::MultiSet(pairs) => pairs
                .iter()
                .map(|(key, value)| key.len() + value.len())
                .sum(),
            _ => self.keys().iter().map(|key| key.len()).sum(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum ClientCommand {
    GetName,
    SetName(Bytes),
    SetInfo { library_name: bool, value: Bytes },
}

#[derive(Clone, Debug)]
pub enum MetadataCommand {
    All,
    Count,
    Info(Vec<Bytes>),
}

#[derive(Clone, Debug)]
pub enum Command {
    Data(Operation),
    Ping(Option<Bytes>),
    Echo(Bytes),
    Quit,
    Auth {
        username: Option<Bytes>,
        password: Bytes,
    },
    Hello {
        protocol: Option<i64>,
        auth: Option<(Bytes, Bytes)>,
        name: Option<Bytes>,
    },
    Select(i64),
    Client(ClientCommand),
    Metadata(MetadataCommand),
    Info(Option<Bytes>),
}

pub fn parse(arguments: &[Bytes], limits: &ParseLimits) -> Result<Command, CommandError> {
    let Some(name) = arguments.first() else {
        return Err(error("ERR empty command"));
    };
    let spec = SPECS
        .iter()
        .find(|spec| name.eq_ignore_ascii_case(spec.name.as_bytes()))
        .ok_or_else(|| error("ERR unknown command"))?;
    let count = arguments.len() as i64;
    if (spec.arity > 0 && count != spec.arity) || (spec.arity < 0 && count < -spec.arity) {
        return Err(error(&format!(
            "ERR wrong number of arguments for '{}' command",
            spec.name
        )));
    }
    let a = arguments;
    let command = match spec.name {
        "ping" if a.len() <= 2 => Command::Ping(a.get(1).cloned()),
        "echo" => Command::Echo(a[1].clone()),
        "quit" => Command::Quit,
        "get" => Command::Data(Operation::Get(a[1].clone())),
        "set" => Command::Data(parse_set(a)?),
        "incr" | "decr" | "incrby" | "decrby" => Command::Data(Operation::Increment {
            key: a[1].clone(),
            amount: if a.len() == 3 { integer(&a[2])? } else { 1 },
            subtract: spec.name.starts_with("decr"),
        }),
        "del" => Command::Data(Operation::Delete(a[1..].to_vec())),
        "exists" => Command::Data(Operation::Exists(a[1..].to_vec())),
        "mget" => Command::Data(Operation::MultiGet(a[1..].to_vec())),
        "mset" if a.len() % 2 == 1 => Command::Data(Operation::MultiSet(
            a[1..]
                .chunks_exact(2)
                .map(|pair| (pair[0].clone(), pair[1].clone()))
                .collect(),
        )),
        "expire" | "pexpire" => {
            let value = integer(&a[2])?;
            let milliseconds = if spec.name == "expire" {
                value
                    .checked_mul(1000)
                    .ok_or_else(|| error("ERR invalid expire time in 'expire' command"))?
            } else {
                value
            };
            Command::Data(Operation::Expire {
                key: a[1].clone(),
                milliseconds,
            })
        }
        "ttl" | "pttl" => Command::Data(Operation::Ttl {
            key: a[1].clone(),
            milliseconds: spec.name == "pttl",
        }),
        "persist" => Command::Data(Operation::Persist(a[1].clone())),
        "auth" if a.len() == 2 => Command::Auth {
            username: None,
            password: a[1].clone(),
        },
        "auth" if a.len() == 3 => Command::Auth {
            username: Some(a[1].clone()),
            password: a[2].clone(),
        },
        "select" => Command::Select(integer(&a[1])?),
        "hello" => parse_hello(a, limits)?,
        "client" => Command::Client(parse_client(a, limits)?),
        "command" if a.len() == 1 => Command::Metadata(MetadataCommand::All),
        "command" if a[1].eq_ignore_ascii_case(b"count") && a.len() == 2 => {
            Command::Metadata(MetadataCommand::Count)
        }
        "command" if a[1].eq_ignore_ascii_case(b"info") => {
            Command::Metadata(MetadataCommand::Info(a[2..].to_vec()))
        }
        "info" if a.len() <= 2 => Command::Info(a.get(1).cloned()),
        _ => return Err(error("ERR syntax error")),
    };
    if let Command::Data(operation) = &command {
        if operation
            .keys()
            .iter()
            .any(|key| key.len() > limits.max_key_bytes)
        {
            return Err(error("ERR key exceeds configured limit"));
        }
        let value_too_large = match operation {
            Operation::Set { value, .. } => value.len() > limits.max_value_bytes,
            Operation::MultiSet(pairs) => pairs
                .iter()
                .any(|(_, value)| value.len() > limits.max_value_bytes),
            _ => false,
        };
        if value_too_large {
            return Err(error("ERR value exceeds configured limit"));
        }
    }
    Ok(command)
}

fn parse_set(a: &[Bytes]) -> Result<Operation, CommandError> {
    let mut condition = Condition::Always;
    let mut expiry = Expiry::Clear;
    let mut index = 3;
    while index < a.len() {
        let option = &a[index];
        if option.eq_ignore_ascii_case(b"nx") || option.eq_ignore_ascii_case(b"xx") {
            if condition != Condition::Always {
                return Err(error("ERR syntax error"));
            }
            condition = if option.eq_ignore_ascii_case(b"nx") {
                Condition::IfAbsent
            } else {
                Condition::IfPresent
            };
        } else if option.eq_ignore_ascii_case(b"keepttl") {
            if expiry != Expiry::Clear {
                return Err(error("ERR syntax error"));
            }
            expiry = Expiry::Keep;
        } else if option.eq_ignore_ascii_case(b"ex") || option.eq_ignore_ascii_case(b"px") {
            if expiry != Expiry::Clear || index + 1 == a.len() {
                return Err(error("ERR syntax error"));
            }
            index += 1;
            let mut duration = integer(&a[index])?;
            if option.eq_ignore_ascii_case(b"ex") {
                duration = duration
                    .checked_mul(1000)
                    .ok_or_else(|| error("ERR invalid expire time in 'set' command"))?;
            }
            if duration <= 0 {
                return Err(error("ERR invalid expire time in 'set' command"));
            }
            expiry = Expiry::AfterMilliseconds(duration);
        } else {
            return Err(error("ERR syntax error"));
        }
        index += 1;
    }
    Ok(Operation::Set {
        key: a[1].clone(),
        value: a[2].clone(),
        condition,
        expiry,
    })
}

fn parse_hello(a: &[Bytes], limits: &ParseLimits) -> Result<Command, CommandError> {
    let protocol = a.get(1).map(|value| integer(value)).transpose()?;
    let mut auth = None;
    let mut name = None;
    let mut index = 2;
    while index < a.len() {
        if a[index].eq_ignore_ascii_case(b"auth") && auth.is_none() && index + 2 < a.len() {
            auth = Some((a[index + 1].clone(), a[index + 2].clone()));
            index += 3;
        } else if a[index].eq_ignore_ascii_case(b"setname") && name.is_none() && index + 1 < a.len()
        {
            client_name(&a[index + 1], limits)?;
            name = Some(a[index + 1].clone());
            index += 2;
        } else {
            return Err(error("ERR syntax error"));
        }
    }
    Ok(Command::Hello {
        protocol,
        auth,
        name,
    })
}

fn parse_client(a: &[Bytes], limits: &ParseLimits) -> Result<ClientCommand, CommandError> {
    if a[1].eq_ignore_ascii_case(b"getname") && a.len() == 2 {
        Ok(ClientCommand::GetName)
    } else if a[1].eq_ignore_ascii_case(b"setname") && a.len() == 3 {
        client_name(&a[2], limits)?;
        Ok(ClientCommand::SetName(a[2].clone()))
    } else if a[1].eq_ignore_ascii_case(b"setinfo") && a.len() == 4 {
        client_name(&a[3], limits)?;
        let library_name = if a[2].eq_ignore_ascii_case(b"lib-name") {
            true
        } else if a[2].eq_ignore_ascii_case(b"lib-ver") {
            false
        } else {
            return Err(error("ERR unsupported CLIENT SETINFO attribute"));
        };
        Ok(ClientCommand::SetInfo {
            library_name,
            value: a[3].clone(),
        })
    } else {
        Err(error("ERR unsupported CLIENT subcommand or arguments"))
    }
}

fn client_name(value: &[u8], limits: &ParseLimits) -> Result<(), CommandError> {
    if value.len() > limits.max_client_metadata_bytes
        || value.iter().any(|b| !(33..=126).contains(b))
    {
        return Err(error(
            "ERR Client names cannot contain spaces, newlines or special characters",
        ));
    }
    Ok(())
}

pub fn integer(value: &[u8]) -> Result<i64, CommandError> {
    if value.is_empty() || value.len() > 20 {
        return Err(error("ERR value is not an integer or out of range"));
    }
    let parsed = std::str::from_utf8(value)
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|parsed| parsed.to_string().as_bytes() == value)
        .ok_or_else(|| error("ERR value is not an integer or out of range"))?;
    Ok(parsed)
}

fn error(message: &str) -> CommandError {
    CommandError(message.to_owned())
}

pub struct CommandSpec {
    pub name: &'static str,
    pub arity: i64,
    pub write: bool,
    pub first_key: i64,
    pub last_key: i64,
    pub key_step: i64,
}

macro_rules! spec {
    ($name:literal, $arity:expr, $write:expr, $first:expr, $last:expr, $step:expr) => {
        CommandSpec {
            name: $name,
            arity: $arity,
            write: $write,
            first_key: $first,
            last_key: $last,
            key_step: $step,
        }
    };
}

pub static SPECS: &[CommandSpec] = &[
    spec!("ping", -1, false, 0, 0, 0),
    spec!("echo", 2, false, 0, 0, 0),
    spec!("quit", 1, false, 0, 0, 0),
    spec!("auth", -2, false, 0, 0, 0),
    spec!("hello", -1, false, 0, 0, 0),
    spec!("select", 2, false, 0, 0, 0),
    spec!("get", 2, false, 1, 1, 1),
    spec!("set", -3, true, 1, 1, 1),
    spec!("incr", 2, true, 1, 1, 1),
    spec!("incrby", 3, true, 1, 1, 1),
    spec!("decr", 2, true, 1, 1, 1),
    spec!("decrby", 3, true, 1, 1, 1),
    spec!("del", -2, true, 1, -1, 1),
    spec!("exists", -2, false, 1, -1, 1),
    spec!("mget", -2, false, 1, -1, 1),
    spec!("mset", -3, true, 1, -1, 2),
    spec!("expire", 3, true, 1, 1, 1),
    spec!("pexpire", 3, true, 1, 1, 1),
    spec!("ttl", 2, false, 1, 1, 1),
    spec!("pttl", 2, false, 1, 1, 1),
    spec!("persist", 2, true, 1, 1, 1),
    spec!("command", -1, false, 0, 0, 0),
    spec!("client", -2, false, 0, 0, 0),
    spec!("info", -1, false, 0, 0, 0),
];

pub fn metadata(command: MetadataCommand) -> Reply {
    match command {
        MetadataCommand::Count => Reply::Integer(SPECS.len() as i64),
        MetadataCommand::All => Reply::Array(SPECS.iter().map(spec_reply).collect()),
        MetadataCommand::Info(names) if names.is_empty() => {
            Reply::Array(SPECS.iter().map(spec_reply).collect())
        }
        MetadataCommand::Info(names) => Reply::Array(
            names
                .iter()
                .map(|name| {
                    SPECS
                        .iter()
                        .find(|spec| name.eq_ignore_ascii_case(spec.name.as_bytes()))
                        .map(spec_reply)
                        .unwrap_or(Reply::Bulk(None))
                })
                .collect(),
        ),
    }
}

fn spec_reply(spec: &CommandSpec) -> Reply {
    let flags = if spec.first_key == 0 {
        vec![]
    } else {
        vec![Reply::Simple(Bytes::from_static(if spec.write {
            b"write"
        } else {
            b"readonly"
        }))]
    };
    Reply::Array(vec![
        Reply::bulk(spec.name),
        Reply::Integer(spec.arity),
        Reply::Array(flags),
        Reply::Integer(spec.first_key),
        Reply::Integer(spec.last_key),
        Reply::Integer(spec.key_step),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<Bytes> {
        values
            .iter()
            .map(|s| Bytes::copy_from_slice(s.as_bytes()))
            .collect()
    }

    #[test]
    fn canonical_integer_boundaries() {
        assert_eq!(integer(b"-9223372036854775808").unwrap(), i64::MIN);
        assert_eq!(integer(b"9223372036854775807").unwrap(), i64::MAX);
        for invalid in ["", "-0", "+1", "01", " 1", "1 ", "9223372036854775808"] {
            assert!(integer(invalid.as_bytes()).is_err(), "{invalid}");
        }
    }

    #[test]
    fn rejects_ambiguous_or_unsupported_set_options() {
        for options in [
            vec!["NX", "XX"],
            vec!["NX", "NX"],
            vec!["PX", "0"],
            vec!["EX", "-1"],
            vec!["PX", "1", "KEEPTTL"],
            vec!["GET"],
            vec!["EX"],
        ] {
            let mut command = vec!["SET", "key", "value"];
            command.extend(options);
            assert!(parse(&args(&command), &ParseLimits::default()).is_err());
        }
    }

    #[test]
    fn command_names_and_options_are_case_insensitive() {
        assert!(matches!(
            parse(
                &args(&["sEt", "k", "v", "nX", "pX", "10"]),
                &ParseLimits::default()
            )
            .unwrap(),
            Command::Data(Operation::Set {
                condition: Condition::IfAbsent,
                ..
            })
        ));
    }

    #[test]
    fn validates_full_multi_assignment_before_execution() {
        assert!(parse(&args(&["MSET", "a", "1", "b"]), &ParseLimits::default()).is_err());
        assert!(parse(&args(&["MULTI"]), &ParseLimits::default()).is_err());
    }

    #[test]
    fn empty_unknown_and_arity_violations_are_rejected() {
        assert_eq!(
            parse(&[], &ParseLimits::default()).unwrap_err().0,
            "ERR empty command"
        );
        assert_eq!(
            parse(&args(&["WHATEVER"]), &ParseLimits::default())
                .unwrap_err()
                .0,
            "ERR unknown command"
        );
        assert_eq!(
            parse(&args(&["GET", "k", "extra"]), &ParseLimits::default())
                .unwrap_err()
                .0,
            "ERR wrong number of arguments for 'get' command"
        );
        assert_eq!(
            parse(&args(&["ECHO"]), &ParseLimits::default())
                .unwrap_err()
                .0,
            "ERR wrong number of arguments for 'echo' command"
        );
        assert_eq!(
            parse(&args(&["PING", "a", "b", "c"]), &ParseLimits::default())
                .unwrap_err()
                .0,
            "ERR syntax error"
        );
        assert!(parse(&args(&["COMMAND", "DOCS"]), &ParseLimits::default()).is_err());
    }

    #[test]
    fn key_and_value_limits_apply_at_exact_boundaries() {
        let limits = ParseLimits {
            max_key_bytes: 2,
            max_value_bytes: 3,
            ..ParseLimits::default()
        };
        assert!(parse(&args(&["SET", "ab", "123"]), &limits).is_ok());
        assert_eq!(
            parse(&args(&["SET", "abc", "1"]), &limits).unwrap_err().0,
            "ERR key exceeds configured limit"
        );
        assert_eq!(
            parse(&args(&["SET", "a", "1234"]), &limits).unwrap_err().0,
            "ERR value exceeds configured limit"
        );
        // Every key and value in a multi-key command is checked, not the first.
        assert_eq!(
            parse(&args(&["DEL", "a", "abc"]), &limits).unwrap_err().0,
            "ERR key exceeds configured limit"
        );
        assert_eq!(
            parse(&args(&["MSET", "a", "1", "bb", "1234"]), &limits)
                .unwrap_err()
                .0,
            "ERR value exceeds configured limit"
        );
        assert!(parse(&args(&["MGET", "ab", "cd"]), &limits).is_ok());
    }

    #[test]
    fn expire_and_integer_overflows_are_rejected_before_storage() {
        // Integers longer than 20 bytes never reach parsing.
        assert!(integer(b"100000000000000000000").is_err());
        assert_eq!(
            parse(
                &args(&["EXPIRE", "k", "9223372036854775807"]),
                &ParseLimits::default()
            )
            .unwrap_err()
            .0,
            "ERR invalid expire time in 'expire' command"
        );
        assert_eq!(
            parse(
                &args(&["SET", "k", "v", "EX", "9223372036854775807"]),
                &ParseLimits::default()
            )
            .unwrap_err()
            .0,
            "ERR invalid expire time in 'set' command"
        );
        // PEXPIRE passes large values through; the store bounds the deadline.
        assert!(matches!(
            parse(
                &args(&["PEXPIRE", "k", "9223372036854775807"]),
                &ParseLimits::default()
            )
            .unwrap(),
            Command::Data(Operation::Expire {
                milliseconds: i64::MAX,
                ..
            })
        ));
        // Negative expirations parse; the store treats them as deletion.
        assert!(matches!(
            parse(&args(&["EXPIRE", "k", "-5"]), &ParseLimits::default()).unwrap(),
            Command::Data(Operation::Expire {
                milliseconds: -5000,
                ..
            })
        ));
    }

    #[test]
    fn hello_and_client_options_are_validated_at_parse_time() {
        assert!(matches!(
            parse(&args(&["HELLO"]), &ParseLimits::default()).unwrap(),
            Command::Hello {
                protocol: None,
                auth: None,
                name: None
            }
        ));
        assert!(matches!(
            parse(&args(&["HELLO", "2"]), &ParseLimits::default()).unwrap(),
            Command::Hello {
                protocol: Some(2),
                ..
            }
        ));
        assert!(parse(&args(&["HELLO", "two"]), &ParseLimits::default()).is_err());
        // AUTH requires exactly a username and password; repeats are rejected.
        assert!(parse(
            &args(&["HELLO", "2", "AUTH", "default"]),
            &ParseLimits::default()
        )
        .is_err());
        assert!(parse(
            &args(&["HELLO", "2", "AUTH", "a", "b", "AUTH", "a", "b"]),
            &ParseLimits::default()
        )
        .is_err());
        assert!(matches!(
            parse(
                &args(&["HELLO", "2", "AUTH", "u", "p", "SETNAME", "n"]),
                &ParseLimits::default()
            )
            .unwrap(),
            Command::Hello {
                protocol: Some(2),
                auth: Some(_),
                name: Some(_)
            }
        ));
        assert!(matches!(
            parse(
                &args(&["CLIENT", "SETNAME", "ok-name"]),
                &ParseLimits::default()
            )
            .unwrap(),
            Command::Client(ClientCommand::SetName(_))
        ));
        assert!(matches!(
            parse(
                &args(&["CLIENT", "SETINFO", "LIB-NAME", "lib"]),
                &ParseLimits::default()
            )
            .unwrap(),
            Command::Client(ClientCommand::SetInfo {
                library_name: true,
                ..
            })
        ));
        assert!(matches!(
            parse(
                &args(&["CLIENT", "SETINFO", "lib-ver", "1.0"]),
                &ParseLimits::default()
            )
            .unwrap(),
            Command::Client(ClientCommand::SetInfo {
                library_name: false,
                ..
            })
        ));
        assert_eq!(
            parse(
                &args(&["CLIENT", "SETINFO", "bogus", "x"]),
                &ParseLimits::default()
            )
            .unwrap_err()
            .0,
            "ERR unsupported CLIENT SETINFO attribute"
        );
        assert!(parse(&args(&["CLIENT", "bogus"]), &ParseLimits::default()).is_err());
    }

    #[test]
    fn client_name_boundaries_are_enforced() {
        let limits = ParseLimits {
            max_client_metadata_bytes: 2,
            ..ParseLimits::default()
        };
        assert!(parse(&args(&["CLIENT", "SETNAME", "ab"]), &limits).is_ok());
        assert!(parse(&args(&["CLIENT", "SETNAME", "abc"]), &limits).is_err());
        // Only printable non-space ASCII is accepted: 33 and 126 pass.
        assert!(parse(&args(&["CLIENT", "SETNAME", "!~"]), &ParseLimits::default()).is_ok());
        for invalid in ["bad name", "\x7f", "tab\t", "new\n"] {
            assert!(
                parse(
                    &args(&["CLIENT", "SETNAME", invalid]),
                    &ParseLimits::default()
                )
                .is_err(),
                "{invalid:?}"
            );
        }
        // Empty names parse; the session layer normalizes them away.
        assert!(parse(&args(&["CLIENT", "SETNAME", ""]), &ParseLimits::default()).is_ok());
    }

    #[test]
    fn command_metadata_stays_consistent_with_the_spec_table() {
        let Reply::Array(all) = metadata(MetadataCommand::All) else {
            panic!("expected an array")
        };
        assert_eq!(
            metadata(MetadataCommand::Count),
            Reply::Integer(all.len() as i64)
        );
        assert_eq!(
            metadata(MetadataCommand::Info(vec![])),
            metadata(MetadataCommand::All)
        );
        let Reply::Array(info) = metadata(MetadataCommand::Info(vec![
            Bytes::from_static(b"GET"),
            Bytes::from_static(b"nosuch"),
        ])) else {
            panic!("expected an array")
        };
        assert_eq!(info.len(), 2);
        // Name matching is case-insensitive and answers with the canonical name.
        assert_eq!(
            info[0],
            Reply::Array(vec![
                Reply::bulk("get"),
                Reply::Integer(2),
                Reply::Array(vec![Reply::Simple(Bytes::from_static(b"readonly"))]),
                Reply::Integer(1),
                Reply::Integer(1),
                Reply::Integer(1),
            ])
        );
        assert_eq!(info[1], Reply::Bulk(None));
        // Keyed writes announce the write flag and their multi-key positions.
        let Reply::Array(info) = metadata(MetadataCommand::Info(vec![Bytes::from_static(b"mset")]))
        else {
            panic!("expected an array")
        };
        assert_eq!(
            info[0],
            Reply::Array(vec![
                Reply::bulk("mset"),
                Reply::Integer(-3),
                Reply::Array(vec![Reply::Simple(Bytes::from_static(b"write"))]),
                Reply::Integer(1),
                Reply::Integer(-1),
                Reply::Integer(2),
            ])
        );
        // Commands without keys carry no flags or key positions.
        let Reply::Array(info) = metadata(MetadataCommand::Info(vec![Bytes::from_static(b"ping")]))
        else {
            panic!("expected an array")
        };
        assert_eq!(
            info[0],
            Reply::Array(vec![
                Reply::bulk("ping"),
                Reply::Integer(-1),
                Reply::Array(vec![]),
                Reply::Integer(0),
                Reply::Integer(0),
                Reply::Integer(0),
            ])
        );
    }
}
