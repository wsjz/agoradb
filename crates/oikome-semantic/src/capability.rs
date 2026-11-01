// Copyright 2025 The Oikome Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Identity and capability statements (architecture v3 §9):
//!
//! ```sql
//! SHOW IDENTITY
//! ISSUE UCAN ON SPACE <space> [, <space> ...] | * TO '<did>'
//!     [CAN 'space/query' | 'space/read' | 'space/*'] [EXPIRES IN <duration>]
//! REVOKE UCAN '<token id>'
//! SHOW UCANS
//! ```
//!
//! A duration is a number of seconds or a number with a unit: `90s`,
//! `30m`, `12h`, `7d`, quoted or not.

use crate::error::SemanticError;

/// Default lifetime of an issued UCAN.
pub const DEFAULT_UCAN_LIFETIME_SECS: u64 = 24 * 3600;
/// Longest lifetime `ISSUE UCAN` accepts.
pub const MAX_UCAN_LIFETIME_SECS: u64 = 366 * 24 * 3600;
/// Ability granted when `CAN` is omitted.
pub const DEFAULT_ABILITY: &str = "space/query";

/// What `ISSUE UCAN` asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IssueUcanRequest {
    /// Space names, or `["*"]` for every Space of the node.
    pub spaces: Vec<String>,
    /// DID of the recipient.
    pub audience: String,
    pub ability: String,
    pub lifetime_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Tok {
    Word(String),
    Quoted(String),
    Comma,
}

impl Tok {
    fn is_kw(&self, kw: &str) -> bool {
        matches!(self, Tok::Word(w) if w.eq_ignore_ascii_case(kw))
    }

    fn value(&self) -> Option<&str> {
        match self {
            Tok::Word(w) | Tok::Quoted(w) => Some(w),
            Tok::Comma => None,
        }
    }
}

fn err(stmt: &str, msg: impl AsRef<str>) -> SemanticError {
    SemanticError::Parse(format!("{stmt}: {}", msg.as_ref()))
}

fn tokenize(input: &str, stmt: &str) -> Result<Vec<Tok>, SemanticError> {
    let mut tokens = Vec::new();
    let mut chars = input.trim().trim_end_matches(';').chars().peekable();
    while let Some(&c) = chars.peek() {
        match c {
            c if c.is_whitespace() => {
                chars.next();
            }
            ',' => {
                chars.next();
                tokens.push(Tok::Comma);
            }
            '\'' | '"' => {
                chars.next();
                let mut value = String::new();
                loop {
                    match chars.next() {
                        Some(q) if q == c => {
                            // A doubled quote is an escaped quote.
                            if chars.peek() == Some(&c) {
                                chars.next();
                                value.push(c);
                            } else {
                                break;
                            }
                        }
                        Some(other) => value.push(other),
                        None => return Err(err(stmt, "unterminated string")),
                    }
                }
                tokens.push(Tok::Quoted(value));
            }
            _ => {
                let mut word = String::new();
                while let Some(&c) = chars.peek() {
                    if c.is_whitespace() || c == ',' || c == '\'' || c == '"' {
                        break;
                    }
                    word.push(c);
                    chars.next();
                }
                tokens.push(Tok::Word(word));
            }
        }
    }
    Ok(tokens)
}

/// Parse a duration: seconds, or a number followed by `s`, `m`, `h` or `d`.
pub fn parse_duration(text: &str) -> Option<u64> {
    let text = text.trim();
    let (digits, unit) = match text.char_indices().find(|(_, c)| !c.is_ascii_digit()) {
        Some((i, _)) => (&text[..i], text[i..].trim()),
        None => (text, "s"),
    };
    let n: u64 = digits.parse().ok()?;
    let scale = match unit.to_ascii_lowercase().as_str() {
        "s" | "sec" | "secs" | "second" | "seconds" => 1,
        "m" | "min" | "mins" | "minute" | "minutes" => 60,
        "h" | "hour" | "hours" => 3600,
        "d" | "day" | "days" => 86_400,
        _ => return None,
    };
    n.checked_mul(scale)
}

fn space_name(tok: &Tok, stmt: &str) -> Result<String, SemanticError> {
    let name = tok
        .value()
        .ok_or_else(|| err(stmt, "expected a space name"))?;
    let ok = name == "*"
        || (!name.is_empty()
            && name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
            && !name.starts_with(|c: char| c.is_ascii_digit()));
    if ok {
        Ok(name.to_string())
    } else {
        Err(err(stmt, format!("invalid space name '{name}'")))
    }
}

/// `ISSUE UCAN ...`
pub(crate) fn parse_issue_ucan(sql: &str) -> Result<IssueUcanRequest, SemanticError> {
    const STMT: &str = "ISSUE UCAN";
    let toks = tokenize(sql, STMT)?;
    let mut i = 2; // ISSUE UCAN
    let expect = |kw: &str, i: &mut usize| -> Result<(), SemanticError> {
        if toks.get(*i).is_some_and(|t| t.is_kw(kw)) {
            *i += 1;
            Ok(())
        } else {
            Err(err(STMT, format!("expected {kw}")))
        }
    };
    expect("ON", &mut i)?;
    expect("SPACE", &mut i)?;

    let mut spaces = Vec::new();
    loop {
        let tok = toks
            .get(i)
            .ok_or_else(|| err(STMT, "expected a space name"))?;
        spaces.push(space_name(tok, STMT)?);
        i += 1;
        if toks.get(i) == Some(&Tok::Comma) {
            i += 1;
        } else {
            break;
        }
    }
    if spaces.len() > 1 && spaces.iter().any(|s| s == "*") {
        return Err(err(STMT, "'*' cannot be combined with space names"));
    }

    expect("TO", &mut i)?;
    let audience = match toks.get(i) {
        Some(Tok::Quoted(did)) => did.clone(),
        _ => return Err(err(STMT, "expected TO '<did>' (quoted)")),
    };
    i += 1;

    let mut ability = None;
    let mut lifetime = None;
    while i < toks.len() {
        if toks[i].is_kw("CAN") && ability.is_none() {
            ability = Some(
                toks.get(i + 1)
                    .and_then(Tok::value)
                    .ok_or_else(|| err(STMT, "expected CAN '<ability>'"))?
                    .to_string(),
            );
            i += 2;
        } else if toks[i].is_kw("EXPIRES") && lifetime.is_none() {
            if !toks.get(i + 1).is_some_and(|t| t.is_kw("IN")) {
                return Err(err(STMT, "expected EXPIRES IN <duration>"));
            }
            let text = toks
                .get(i + 2)
                .and_then(Tok::value)
                .ok_or_else(|| err(STMT, "expected a duration after EXPIRES IN"))?;
            // `EXPIRES IN 12 hours`
            let (text, used) = match toks.get(i + 3) {
                Some(Tok::Word(unit))
                    if text.chars().all(|c| c.is_ascii_digit())
                        && parse_duration(&format!("1{unit}")).is_some() =>
                {
                    (format!("{text}{unit}"), 4)
                }
                _ => (text.to_string(), 3),
            };
            let secs = parse_duration(&text)
                .ok_or_else(|| err(STMT, format!("invalid duration '{text}'")))?;
            if secs == 0 || secs > MAX_UCAN_LIFETIME_SECS {
                return Err(SemanticError::InvalidProperty(format!(
                    "UCAN lifetime must be between 1 second and {} days",
                    MAX_UCAN_LIFETIME_SECS / 86_400
                )));
            }
            lifetime = Some(secs);
            i += used;
        } else {
            return Err(err(
                STMT,
                format!("unexpected '{}'", toks[i].value().unwrap_or(",")),
            ));
        }
    }

    Ok(IssueUcanRequest {
        spaces,
        audience,
        ability: ability.unwrap_or_else(|| DEFAULT_ABILITY.to_string()),
        lifetime_secs: lifetime.unwrap_or(DEFAULT_UCAN_LIFETIME_SECS),
    })
}

/// `REVOKE UCAN '<id>'`
pub(crate) fn parse_revoke_ucan(sql: &str) -> Result<String, SemanticError> {
    const STMT: &str = "REVOKE UCAN";
    let toks = tokenize(sql, STMT)?;
    match &toks[2..] {
        [tok] => tok
            .value()
            .map(str::to_string)
            .ok_or_else(|| err(STMT, "expected a token id")),
        [] => Err(err(STMT, "expected a token id")),
        _ => Err(err(STMT, "expected a single token id")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations() {
        assert_eq!(parse_duration("90"), Some(90));
        assert_eq!(parse_duration("90s"), Some(90));
        assert_eq!(parse_duration("30m"), Some(1800));
        assert_eq!(parse_duration("12 hours"), Some(43_200));
        assert_eq!(parse_duration("7d"), Some(604_800));
        assert_eq!(parse_duration("1w"), None);
        assert_eq!(parse_duration("h"), None);
        assert_eq!(parse_duration("-1"), None);
        assert_eq!(parse_duration("99999999999999999999d"), None);
    }

    #[test]
    fn issue_ucan_full_and_defaults() {
        let req = parse_issue_ucan(
            "ISSUE UCAN ON SPACE blog, orders TO 'did:key:z6Mkabc' CAN 'space/read' EXPIRES IN '2h';",
        )
        .unwrap();
        assert_eq!(
            req,
            IssueUcanRequest {
                spaces: vec!["blog".into(), "orders".into()],
                audience: "did:key:z6Mkabc".into(),
                ability: "space/read".into(),
                lifetime_secs: 7200,
            }
        );
        let req = parse_issue_ucan("issue ucan on space * to \"did:key:z6Mkabc\"").unwrap();
        assert_eq!(req.spaces, vec!["*".to_string()]);
        assert_eq!(req.ability, DEFAULT_ABILITY);
        assert_eq!(req.lifetime_secs, DEFAULT_UCAN_LIFETIME_SECS);
        let req = parse_issue_ucan(
            "ISSUE UCAN ON SPACE blog TO 'did:key:z' EXPIRES IN 12 hours CAN 'space/*'",
        )
        .unwrap();
        assert_eq!(req.lifetime_secs, 43_200);
        assert_eq!(req.ability, "space/*");
    }

    #[test]
    fn issue_ucan_errors() {
        for bad in [
            "ISSUE UCAN",
            "ISSUE UCAN ON blog TO 'did'",
            "ISSUE UCAN ON SPACE TO 'did'",
            "ISSUE UCAN ON SPACE blog",
            "ISSUE UCAN ON SPACE blog TO did:key:z",
            "ISSUE UCAN ON SPACE blog, * TO 'did'",
            "ISSUE UCAN ON SPACE a.b TO 'did'",
            "ISSUE UCAN ON SPACE blog TO 'did' EXPIRES 1h",
            "ISSUE UCAN ON SPACE blog TO 'did' EXPIRES IN 'soon'",
            "ISSUE UCAN ON SPACE blog TO 'did' EXPIRES IN 0",
            "ISSUE UCAN ON SPACE blog TO 'did' EXPIRES IN 400d",
            "ISSUE UCAN ON SPACE blog TO 'did' CAN",
            "ISSUE UCAN ON SPACE blog TO 'did' CAN 'a' CAN 'b'",
            "ISSUE UCAN ON SPACE blog TO 'did' WITH x",
            "ISSUE UCAN ON SPACE blog TO 'did",
        ] {
            assert!(parse_issue_ucan(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn revoke_ucan() {
        assert_eq!(
            parse_revoke_ucan("REVOKE UCAN '0123456789abcdef0123456789abcdef'").unwrap(),
            "0123456789abcdef0123456789abcdef"
        );
        assert_eq!(parse_revoke_ucan("revoke ucan abc;").unwrap(), "abc");
        assert!(parse_revoke_ucan("REVOKE UCAN").is_err());
        assert!(parse_revoke_ucan("REVOKE UCAN a b").is_err());
    }
}
