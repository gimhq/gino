//! DNS answer decisions for guest queries.

use std::net::{IpAddr, Ipv4Addr};
use std::time::Instant;

use super::pool::{FakePool, PoolError};
use super::rules::{RuleSet, normalize_name};

/// Record type of a guest query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryType {
    /// IPv4 address record.
    A,
    /// IPv6 address record.
    Aaaa,
}

/// What the responder should answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DnsAnswer {
    /// NOERROR with this pool address.
    Allowed(Ipv4Addr),
    /// NOERROR with an empty answer section.
    NoData,
    /// NXDOMAIN.
    NxDomain,
}

/// Decides the answer for `name`. Names not allowed by `rules` (and IP
/// literals) get `NxDomain`; allowed A queries get a pool address; allowed
/// AAAA queries get `NoData` so clients fall back to IPv4, without allocating.
/// Pool exhaustion is returned as the error (the responder answers SERVFAIL).
pub fn answer(
    name: &str,
    qtype: QueryType,
    rules: &RuleSet,
    pool: &mut FakePool,
    now: Instant,
) -> Result<DnsAnswer, PoolError> {
    let name = normalize_name(name);
    if name.parse::<IpAddr>().is_ok() || !rules.name_allowed(&name) {
        return Ok(DnsAnswer::NxDomain);
    }
    match qtype {
        QueryType::A => pool.allocate(&name, now).map(DnsAnswer::Allowed),
        QueryType::Aaaa => Ok(DnsAnswer::NoData),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::super::pool::PoolConfig;
    use super::super::rules::{AlwaysAllow, Rule};
    use super::*;

    fn rules() -> RuleSet {
        let rules: Vec<Rule> = [
            r#"{"host":"github.com","ports":[22,443]}"#,
            r#"{"host":"*.npmjs.org","ports":[443]}"#,
            r#"{"cidr":"10.0.0.0/8","ports":"*"}"#,
        ]
        .iter()
        .map(|r| serde_json::from_str(r).unwrap())
        .collect();
        let always = [AlwaysAllow {
            host: "api.anthropic.com".to_owned(),
            port: 443,
        }];
        RuleSet::compile(&rules, &always).unwrap()
    }

    fn pool(capacity: usize) -> FakePool {
        FakePool::new(PoolConfig {
            capacity,
            idle_reuse: Duration::from_mins(1),
        })
        .unwrap()
    }

    #[test]
    fn allowed_a_gets_stable_pool_address() {
        let (r, mut p, t) = (rules(), pool(8), Instant::now());
        let DnsAnswer::Allowed(addr) = answer("github.com", QueryType::A, &r, &mut p, t).unwrap()
        else {
            panic!("expected an address");
        };
        assert!(FakePool::contains(addr));
        assert_eq!(p.lookup(addr, t).as_deref(), Some("github.com"));
        assert_eq!(
            answer("GitHub.com.", QueryType::A, &r, &mut p, t),
            Ok(DnsAnswer::Allowed(addr))
        );
        assert_eq!(p.len(), 1);
    }

    #[test]
    fn allowed_aaaa_is_nodata_without_allocation() {
        let (r, mut p, t) = (rules(), pool(8), Instant::now());
        assert_eq!(
            answer("github.com", QueryType::Aaaa, &r, &mut p, t),
            Ok(DnsAnswer::NoData)
        );
        assert!(p.is_empty());
    }

    #[test]
    fn disallowed_is_nxdomain() {
        let (r, mut p, t) = (rules(), pool(8), Instant::now());
        for qtype in [QueryType::A, QueryType::Aaaa] {
            for name in ["gitlab.com", "npmjs.org", "x.github.com"] {
                assert_eq!(
                    answer(name, qtype, &r, &mut p, t),
                    Ok(DnsAnswer::NxDomain),
                    "{name}"
                );
            }
        }
        assert!(p.is_empty());
    }

    #[test]
    fn wildcard_and_always_allowed() {
        let (r, mut p, t) = (rules(), pool(8), Instant::now());
        for name in ["registry.npmjs.org", "api.anthropic.com"] {
            assert!(matches!(
                answer(name, QueryType::A, &r, &mut p, t),
                Ok(DnsAnswer::Allowed(_))
            ));
        }
        assert_eq!(p.len(), 2);
    }

    #[test]
    fn ip_literal_is_nxdomain() {
        let (r, mut p, t) = (rules(), pool(8), Instant::now());
        for name in ["10.0.0.1", "198.18.0.1", "::1", "fd00::1."] {
            for qtype in [QueryType::A, QueryType::Aaaa] {
                assert_eq!(
                    answer(name, qtype, &r, &mut p, t),
                    Ok(DnsAnswer::NxDomain),
                    "{name}"
                );
            }
        }
        assert!(p.is_empty());
    }

    #[test]
    fn exhaustion_propagates() {
        let (r, mut p, t) = (rules(), pool(1), Instant::now());
        answer("github.com", QueryType::A, &r, &mut p, t).unwrap();
        assert_eq!(
            answer("registry.npmjs.org", QueryType::A, &r, &mut p, t),
            Err(PoolError::Exhausted { capacity: 1 })
        );
    }
}
