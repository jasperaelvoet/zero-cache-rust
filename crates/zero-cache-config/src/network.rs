//! Port of `zero-cache/src/config/network.ts`'s `getPreferredIp` — the
//! pure interface-ranking half of `getHostIp` (used by
//! `normalize::normalize_zero_config`'s `host_ip` parameter, taken as an
//! injected value in this port rather than reading `os.networkInterfaces()`
//! directly — see `normalize.rs`'s module doc for why).
//!
//! Scope: `getHostIp` itself (the `os.networkInterfaces()` call +
//! logging) is NOT ported — this port has no equivalent OS network-
//! interface enumeration call wired up, and `normalize_zero_config`
//! already takes `host_ip` as a parameter rather than resolving it
//! internally. `getPreferredIp`'s ranking algorithm is fully pure given a
//! list of interfaces, so it's portable on its own.
//!
//! Scope deviation, documented: upstream's `isPrivate`/`isReserved`
//! (from the `is-in-subnet` npm package) classify addresses against the
//! full IANA special-purpose address registry. This port implements an
//! equivalent classification using `std::net`'s built-in address-kind
//! predicates (`is_loopback`/`is_unspecified`/`is_multicast`/
//! `is_link_local`/`is_broadcast`/`is_documentation` for IPv4;
//! hand-rolled ULA (`fc00::/7`) detection for IPv6, since
//! `Ipv6Addr::is_unique_local` isn't stable on this port's MSRV) rather
//! than porting `is-in-subnet`'s exact subnet tables — covers the
//! common/practical cases `getHostIp` actually needs to avoid (loopback,
//! link-local, multicast, unspecified), not necessarily byte-for-byte
//! identical to the npm package's full registry for obscure ranges.

use std::net::IpAddr;

pub const DEFAULT_PREFERRED_PREFIXES: [&str; 2] = ["eth", "en"];

/// One network interface's address info — the fields `getPreferredIp`
/// reads from Node's `NetworkInterfaceInfo`.
#[derive(Debug, Clone, PartialEq)]
pub struct InterfaceInfo {
    pub name: String,
    pub address: String,
    pub internal: bool,
    pub is_ipv4: bool,
}

/// Port of `isReserved` (approximated — see module doc): loopback,
/// unspecified, link-local, multicast, broadcast, or documentation/test
/// ranges.
fn is_reserved(addr: &IpAddr) -> bool {
    match addr {
        IpAddr::V4(v4) => {
            v4.is_loopback()
                || v4.is_unspecified()
                || v4.is_link_local()
                || v4.is_multicast()
                || v4.is_broadcast()
                || v4.is_documentation()
        }
        IpAddr::V6(v6) => {
            v6.is_loopback() || v6.is_unspecified() || v6.is_multicast() || is_ipv6_link_local(v6)
        }
    }
}

fn is_ipv6_link_local(v6: &std::net::Ipv6Addr) -> bool {
    (v6.segments()[0] & 0xffc0) == 0xfe80
}

/// Port of `isPrivate` for IPv6 — Unique Local Address range `fc00::/7`
/// (hand-rolled since `Ipv6Addr::is_unique_local` isn't stable here).
fn is_ipv6_private(v6: &std::net::Ipv6Addr) -> bool {
    (v6.segments()[0] & 0xfe00) == 0xfc00
}

/// Port of the `rank`/sort logic inside `getPreferredIp`: whether this
/// address should be deprioritized as "private-or-reserved" per upstream's
/// `(isIPv6(addr) && isPrivate(addr)) || isReserved(addr)` — note IPv4
/// private (RFC1918) addresses are NOT deprioritized by this check on
/// their own, only IPv6 ULA and anything reserved for either family.
fn is_private_or_reserved(addr: &IpAddr) -> bool {
    let is_private = matches!(addr, IpAddr::V6(v6) if is_ipv6_private(v6));
    is_private || is_reserved(addr)
}

fn prefix_rank(name: &str, preferred_prefixes: &[&str]) -> usize {
    preferred_prefixes
        .iter()
        .position(|p| name.starts_with(p))
        .unwrap_or(usize::MAX)
}

/// Port of `getPreferredIp`. Returns `None` if `interfaces` is empty
/// (upstream would throw on `sorted[0]` being `undefined`, ported as a
/// `None` return instead of a panic, since an empty list is a plausible,
/// non-buggy runtime condition — e.g. a sandboxed container with no
/// interfaces reported — not a programmer error).
pub fn get_preferred_ip(
    interfaces: &[InterfaceInfo],
    preferred_prefixes: &[&str],
) -> Option<String> {
    let mut parsed: Vec<(&InterfaceInfo, IpAddr)> = interfaces
        .iter()
        .filter_map(|i| i.address.parse::<IpAddr>().ok().map(|addr| (i, addr)))
        .collect();
    parsed.sort_by(|(a, a_addr), (b, b_addr)| {
        cmp_interfaces(a, a_addr, b, b_addr, preferred_prefixes)
    });

    let (first, _) = parsed.first()?;
    Some(if !first.is_ipv4 {
        format!("[{}]", first.address)
    } else {
        first.address.clone()
    })
}

fn cmp_interfaces(
    a: &InterfaceInfo,
    a_addr: &IpAddr,
    b: &InterfaceInfo,
    b_addr: &IpAddr,
    preferred_prefixes: &[&str],
) -> std::cmp::Ordering {
    let ap = is_private_or_reserved(a_addr);
    let bp = is_private_or_reserved(b_addr);
    if ap != bp {
        return if ap {
            std::cmp::Ordering::Greater
        } else {
            std::cmp::Ordering::Less
        };
    }
    if a.internal != b.internal {
        return if a.internal {
            std::cmp::Ordering::Greater
        } else {
            std::cmp::Ordering::Less
        };
    }
    if a.is_ipv4 != b.is_ipv4 {
        return if a.is_ipv4 {
            std::cmp::Ordering::Less
        } else {
            std::cmp::Ordering::Greater
        };
    }
    let rank_a = prefix_rank(&a.name, preferred_prefixes);
    let rank_b = prefix_rank(&b.name, preferred_prefixes);
    if rank_a != rank_b {
        return rank_a.cmp(&rank_b);
    }
    a.address.cmp(&b.address)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn iface(name: &str, address: &str, internal: bool, is_ipv4: bool) -> InterfaceInfo {
        InterfaceInfo {
            name: name.into(),
            address: address.into(),
            internal,
            is_ipv4,
        }
    }

    #[test]
    fn prefers_non_internal_over_internal() {
        let interfaces = vec![
            iface("lo0", "127.0.0.1", true, true),
            iface("en0", "192.168.1.5", false, true),
        ];
        assert_eq!(
            get_preferred_ip(&interfaces, &DEFAULT_PREFERRED_PREFIXES),
            Some("192.168.1.5".to_string())
        );
    }

    #[test]
    fn deprioritizes_reserved_addresses_like_link_local() {
        let interfaces = vec![
            iface("en0", "169.254.1.1", false, true),
            iface("en1", "10.0.0.5", false, true),
        ];
        assert_eq!(
            get_preferred_ip(&interfaces, &DEFAULT_PREFERRED_PREFIXES),
            Some("10.0.0.5".to_string())
        );
    }

    #[test]
    fn prefers_ipv4_over_ipv6() {
        let interfaces = vec![
            iface("en0", "2001:db8::1", false, false),
            iface("en0", "192.168.1.5", false, true),
        ];
        assert_eq!(
            get_preferred_ip(&interfaces, &DEFAULT_PREFERRED_PREFIXES),
            Some("192.168.1.5".to_string())
        );
    }

    #[test]
    fn ipv6_addresses_are_bracketed() {
        let interfaces = vec![iface("en0", "2001:db8::1", false, false)];
        assert_eq!(
            get_preferred_ip(&interfaces, &DEFAULT_PREFERRED_PREFIXES),
            Some("[2001:db8::1]".to_string())
        );
    }

    #[test]
    fn ipv6_unique_local_addresses_are_deprioritized() {
        let interfaces = vec![
            iface("en0", "fd00::1", false, false),
            iface("en1", "2001:db8::1", false, false),
        ];
        assert_eq!(
            get_preferred_ip(&interfaces, &DEFAULT_PREFERRED_PREFIXES),
            Some("[2001:db8::1]".to_string())
        );
    }

    #[test]
    fn prefers_preferred_prefix_order_when_otherwise_tied() {
        let interfaces = vec![
            iface("wlan0", "192.168.1.5", false, true),
            iface("eth0", "192.168.1.6", false, true),
        ];
        assert_eq!(
            get_preferred_ip(&interfaces, &DEFAULT_PREFERRED_PREFIXES),
            Some("192.168.1.6".to_string()),
            "eth should be preferred over wlan"
        );
    }

    #[test]
    fn falls_back_to_address_string_comparison_when_fully_tied() {
        let interfaces = vec![
            iface("eth0", "192.168.1.9", false, true),
            iface("eth1", "192.168.1.2", false, true),
        ];
        assert_eq!(
            get_preferred_ip(&interfaces, &DEFAULT_PREFERRED_PREFIXES),
            Some("192.168.1.2".to_string())
        );
    }

    #[test]
    fn empty_interfaces_returns_none() {
        assert_eq!(get_preferred_ip(&[], &DEFAULT_PREFERRED_PREFIXES), None);
    }

    #[test]
    fn ipv4_private_rfc1918_addresses_are_not_deprioritized_by_privacy_alone() {
        // Matches upstream: only isReserved applies to IPv4 in the rank
        // check, not isPrivate — so a RFC1918 address (192.168/16) ranks
        // the same as a "public-looking" IPv4 as far as this check goes.
        let interfaces = vec![
            iface("en0", "192.168.1.5", false, true),
            iface("en1", "8.8.8.8", false, true),
        ];
        // Tied on privacy/reserved/internal/family -> falls through to
        // prefix rank (both default, tied) -> address string comparison.
        assert_eq!(
            get_preferred_ip(&interfaces, &DEFAULT_PREFERRED_PREFIXES),
            Some("192.168.1.5".to_string())
        );
    }
}
