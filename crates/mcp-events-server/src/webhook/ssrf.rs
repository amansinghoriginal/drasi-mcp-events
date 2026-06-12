//! SSRF / DNS-rebinding hardening for webhook egress (design sketch §Webhook
//! Security → SSRF prevention): hostnames are resolved at delivery time, the
//! resolved address is checked against the IANA special-purpose registries,
//! and the connection is pinned to the validated IP via a reqwest resolve
//! override (original hostname kept for Host/SNI). Redirects are disabled on
//! every client used here, including the shared `AppState.http`.
//!
//! With `webhook.allowInsecureUrls` (LOCAL TESTING ONLY) all checks are
//! bypassed and the shared client is used directly.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;

use mcp_events_engine::error_category;
use url::{Host, Url};

use crate::state::AppState;

#[derive(Debug, thiserror::Error)]
pub enum SsrfError {
    #[error("callback host resolves to a non-globally-routable address ({0})")]
    NotGlobal(IpAddr),
    #[error("callback host did not resolve to any address")]
    NoAddress,
    #[error("callback host resolution failed: {0}")]
    Resolve(std::io::Error),
    #[error("callback URL has no usable host/port")]
    BadUrl,
    #[error("building pinned HTTP client failed: {0}")]
    Client(reqwest::Error),
}

impl SsrfError {
    /// `lastError` category for failures that happen before a connection is
    /// attempted. The sketch defines no dedicated category for DNS failures
    /// or policy rejections; `connection_refused` is the closest fit
    /// (recorded as a spec-gap finding).
    pub fn category(&self) -> &'static str {
        error_category::CONNECTION_REFUSED
    }
}

fn ipv4_is_global(ip: Ipv4Addr) -> bool {
    let o = ip.octets();
    !(ip.is_unspecified()
        || o[0] == 0 // 0.0.0.0/8 "this network"
        || ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local()
        || ip.is_broadcast()
        || ip.is_multicast()
        || ip.is_documentation()
        || (o[0] == 100 && (o[1] & 0xc0) == 64) // 100.64.0.0/10 CGNAT
        || (o[0] == 192 && o[1] == 0 && o[2] == 0) // 192.0.0.0/24 IETF protocol
        || (o[0] == 198 && (o[1] & 0xfe) == 18) // 198.18.0.0/15 benchmarking
        || o[0] >= 240) // 240.0.0.0/4 reserved
}

fn ipv6_is_global(ip: Ipv6Addr) -> bool {
    if let Some(v4) = ip.to_ipv4_mapped() {
        return ipv4_is_global(v4);
    }
    let seg = ip.segments();
    !(ip.is_unspecified()
        || ip.is_loopback()
        || ip.is_multicast()
        || (seg[0] & 0xfe00) == 0xfc00 // fc00::/7 ULA
        || (seg[0] & 0xffc0) == 0xfe80 // fe80::/10 link-local
        || (seg[0] == 0x2001 && seg[1] == 0xdb8)) // 2001:db8::/32 documentation
}

/// Globally-routable check per the IANA IPv4/IPv6 special-purpose registries
/// (hand-rolled; `IpAddr::is_global` is unstable).
pub fn ip_is_global(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => ipv4_is_global(v4),
        IpAddr::V6(v6) => ipv6_is_global(v6),
    }
}

/// Resolves and validates `url`'s host at delivery time, returning a client
/// whose connection is pinned to the validated address.
pub async fn client_for(state: &AppState, url: &Url) -> Result<reqwest::Client, SsrfError> {
    if state.config.webhook.allow_insecure_urls {
        return Ok(state.http.clone());
    }
    let Some(port) = url.port_or_known_default() else {
        return Err(SsrfError::BadUrl);
    };
    match url.host() {
        None => Err(SsrfError::BadUrl),
        Some(Host::Ipv4(ip)) => {
            if ipv4_is_global(ip) {
                // IP-literal URL: no DNS to rebind; the shared client connects
                // to exactly this address.
                Ok(state.http.clone())
            } else {
                Err(SsrfError::NotGlobal(IpAddr::V4(ip)))
            }
        }
        Some(Host::Ipv6(ip)) => {
            if ipv6_is_global(ip) {
                Ok(state.http.clone())
            } else {
                Err(SsrfError::NotGlobal(IpAddr::V6(ip)))
            }
        }
        Some(Host::Domain(domain)) => {
            let addrs: Vec<SocketAddr> = tokio::net::lookup_host((domain, port))
                .await
                .map_err(SsrfError::Resolve)?
                .collect();
            if addrs.is_empty() {
                return Err(SsrfError::NoAddress);
            }
            let Some(pinned) = addrs.iter().find(|a| ip_is_global(a.ip())) else {
                return Err(SsrfError::NotGlobal(addrs[0].ip()));
            };
            reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(Duration::from_secs(5))
                .timeout(Duration::from_secs(10))
                .resolve(domain, *pinned)
                .build()
                .map_err(SsrfError::Client)
        }
    }
}

/// Maps a reqwest send error to a `lastError` category. The sketch does not
/// specify how to classify transport errors; this uses reqwest's flags plus
/// a source-chain scan (recorded as a spec-gap finding).
pub fn classify_send_error(error: &reqwest::Error) -> &'static str {
    if error.is_timeout() {
        return error_category::TIMEOUT;
    }
    let mut source = std::error::Error::source(error);
    while let Some(s) = source {
        if let Some(io) = s.downcast_ref::<std::io::Error>() {
            match io.kind() {
                std::io::ErrorKind::ConnectionRefused => {
                    return error_category::CONNECTION_REFUSED
                }
                std::io::ErrorKind::TimedOut => return error_category::TIMEOUT,
                _ => {}
            }
        }
        let text = s.to_string().to_ascii_lowercase();
        if text.contains("tls") || text.contains("certificate") || text.contains("handshake") {
            return error_category::TLS_ERROR;
        }
        source = s.source();
    }
    error_category::CONNECTION_REFUSED
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v4(s: &str) -> IpAddr {
        s.parse().expect("ipv4")
    }
    fn v6(s: &str) -> IpAddr {
        s.parse().expect("ipv6")
    }

    #[test]
    fn ipv4_special_purpose_ranges_are_not_global() {
        for ip in [
            "0.0.0.0",
            "0.1.2.3",
            "127.0.0.1",
            "127.255.255.254",
            "10.0.0.1",
            "172.16.0.1",
            "172.31.255.255",
            "192.168.1.1",
            "169.254.10.10",
            "100.64.0.1",
            "100.127.255.255",
            "192.0.0.7",
            "192.0.2.1",
            "198.18.0.1",
            "198.19.255.255",
            "198.51.100.1",
            "203.0.113.9",
            "224.0.0.1",
            "255.255.255.255",
            "240.0.0.1",
        ] {
            assert!(!ip_is_global(v4(ip)), "{ip} must be rejected");
        }
    }

    #[test]
    fn ipv4_public_addresses_are_global() {
        for ip in ["1.1.1.1", "8.8.8.8", "93.184.216.34", "100.128.0.1", "172.32.0.1"] {
            assert!(ip_is_global(v4(ip)), "{ip} must be allowed");
        }
    }

    #[test]
    fn ipv6_special_purpose_ranges_are_not_global() {
        for ip in [
            "::",
            "::1",
            "fc00::1",
            "fd12:3456::1",
            "fe80::1",
            "febf::1",
            "ff02::1",
            "2001:db8::1",
            "::ffff:10.0.0.1",
            "::ffff:127.0.0.1",
        ] {
            assert!(!ip_is_global(v6(ip)), "{ip} must be rejected");
        }
    }

    #[test]
    fn ipv6_public_addresses_are_global() {
        for ip in ["2600::1", "2606:4700:4700::1111", "::ffff:1.1.1.1"] {
            assert!(ip_is_global(v6(ip)), "{ip} must be allowed");
        }
    }
}
