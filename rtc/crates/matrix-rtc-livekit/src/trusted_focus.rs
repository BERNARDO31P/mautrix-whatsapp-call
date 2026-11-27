// Copyright 2026 Element Creations Ltd.
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial

//! Optional deployment-owned focus restriction for dedicated service accounts.
use crate::LiveKitTarget;

#[derive(Clone, Debug)]
pub struct TrustedFocus {
    server_name: String,
    service_url: String,
    sfu_url: String,
}

impl TrustedFocus {
    /// The canonical single-SFU deployment exposes these two fixed paths.
    pub fn local_deployment(server_name: &str) -> Result<Self, &'static str> {
        if server_name.is_empty() || server_name.chars().any(|c| {
            c.is_whitespace() || c.is_control() || matches!(c, '/' | '\\' | '@' | '?' | '#' | '%')
        }) {
            return Err("invalid configured MatrixRTC server name");
        }
        let service_url = format!("https://{server_name}/livekit/jwt");
        let sfu_url = format!("wss://{server_name}/livekit/sfu");
        if !matrix_rtc_transport::livekit::valid_service_url(&service_url)
            || !matrix_rtc_transport::livekit::valid_sfu_url(&sfu_url) {
            return Err("invalid configured MatrixRTC focus endpoints");
        }
        Ok(Self { server_name: server_name.to_owned(), service_url, sfu_url })
    }

    pub(crate) fn allows_target(&self, target: &LiveKitTarget) -> bool {
        match target {
            LiveKitTarget::SfuGet { service_url } => {
                service_url.trim_end_matches('/') == self.service_url.as_str()
            }
            LiveKitTarget::CsApi { url, server_name } => {
                server_name == &self.server_name && self.allows_sfu_url(url)
            }
        }
    }

    pub(crate) fn allows_sfu_url(&self, url: &str) -> bool {
        url.trim_end_matches('/') == self.sfu_url.as_str()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_deployment_endpoints_are_allowed() {
        let policy = TrustedFocus::local_deployment("matrix.example.org").unwrap();
        for service_url in ["https://matrix.example.org/livekit/jwt", "https://matrix.example.org/livekit/jwt/"] {
            assert!(policy.allows_target(&LiveKitTarget::SfuGet { service_url: service_url.into() }));
        }
        for service_url in ["https://other.example.org/livekit/jwt", "http://matrix.example.org/livekit/jwt",
            "https://matrix.example.org/livekit/jwt?x=1", "https://matrix.example.org/another/path",
            "https://matrix.example.org:8443/livekit/jwt"] {
            assert!(!policy.allows_target(&LiveKitTarget::SfuGet { service_url: service_url.into() }));
        }
        assert!(policy.allows_sfu_url("wss://matrix.example.org/livekit/sfu/"));
        assert!(!policy.allows_sfu_url("wss://other.example.org/livekit/sfu"));
        assert!(policy.allows_target(&LiveKitTarget::CsApi {
            url: "wss://matrix.example.org/livekit/sfu".into(), server_name: "matrix.example.org".into(),
        }));
        assert!(!policy.allows_target(&LiveKitTarget::CsApi {
            url: "wss://matrix.example.org/livekit/sfu".into(), server_name: "other.example.org".into(),
        }));
        for server_name in ["", "matrix.example.org/path", "matrix.example.org?x", "user@matrix.example.org", "matrix.example.org:0"] {
            assert!(TrustedFocus::local_deployment(server_name).is_err());
        }
    }
}
