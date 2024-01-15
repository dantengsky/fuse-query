// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use common_base::base::GlobalInstance;
use common_exception::exception::ErrorCode;
use common_exception::Result;
use common_exception::ToErrorCode;
use common_license::license::Feature;
use common_license::license::LicenseInfo;
use common_license::license_manager::LicenseManager;
use common_license::license_manager::LicenseManagerWrapper;
use dashmap::DashMap;
use jwt_simple::algorithms::ES256PublicKey;
use jwt_simple::claims::JWTClaims;
use jwt_simple::prelude::Clock;
use jwt_simple::prelude::ECDSAP256PublicKeyLike;

const LICENSE_PUBLIC_KEY: &str = r#"-----BEGIN PUBLIC KEY-----
MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEGsKCbhXU7j56VKZ7piDlLXGhud0a
pWjW3wxSdeARerxs/BeoWK7FspDtfLaAT8iJe4YEmR0JpkRQ8foWs0ve3w==
-----END PUBLIC KEY-----"#;

pub struct RealLicenseManager {
    tenant: String,
    public_key: String,

    // cache available settings to get avoid of unneeded license parsing time.
    pub(crate) cache: DashMap<String, JWTClaims<LicenseInfo>>,
}

impl LicenseManager for RealLicenseManager {
    fn init(tenant: String) -> Result<()> {
        let rm = RealLicenseManager {
            tenant,
            cache: DashMap::new(),
            public_key: LICENSE_PUBLIC_KEY.to_string(),
        };
        let wrapper = LicenseManagerWrapper {
            manager: Box::new(rm),
        };
        GlobalInstance::set(Arc::new(wrapper));
        Ok(())
    }

    fn instance() -> Arc<Box<dyn LicenseManager>> {
        GlobalInstance::get()
    }

    fn check_enterprise_enabled(&self, license_key: String, feature: Feature) -> Result<()> {
        if license_key.is_empty() {
            return Err(ErrorCode::LicenseKeyInvalid(format!(
                "use of {feature} requires an enterprise license. license key is not found for {}",
                self.tenant
            )));
        }

        if let Some(v) = self.cache.get(&license_key) {
            return Self::verify_license(v.value(), feature);
        }

        let license = self.parse_license(&license_key).map_err_to_code(
            ErrorCode::LicenseKeyInvalid,
            || format!("use of {feature} requires an enterprise license. current license is invalid for {}", self.tenant),
        )?;
        Self::verify_feature(&license, feature)?;
        self.cache.insert(license_key, license);
        Ok(())
    }

    fn parse_license(&self, raw: &str) -> Result<JWTClaims<LicenseInfo>> {
        let public_key = ES256PublicKey::from_pem(self.public_key.as_str())
            .map_err_to_code(ErrorCode::LicenseKeyParseError, || "public key load failed")?;
        public_key
            .verify_token::<LicenseInfo>(raw, None)
            .map_err_to_code(
                ErrorCode::LicenseKeyParseError,
                || "jwt claim decode failed",
            )
    }
}

impl RealLicenseManager {
    // this method mainly used for unit tests
    pub fn new(tenant: String, public_key: String) -> Self {
        RealLicenseManager {
            tenant,
            public_key,

            cache: DashMap::new(),
        }
    }

    fn verify_license(l: &JWTClaims<LicenseInfo>, feature: Feature) -> Result<()> {
        let now = Clock::now_since_epoch();
        match l.expires_at {
            Some(expire_at) => {
                if now > expire_at {
                    return Err(ErrorCode::LicenseKeyInvalid(format!(
                        "license key expired in {:?}",
                        expire_at
                    )));
                }
            }
            None => {
                return Err(ErrorCode::LicenseKeyInvalid(
                    "cannot find valid expire time",
                ));
            }
        }
        Self::verify_feature(l, feature)?;
        Ok(())
    }

    fn verify_feature(l: &JWTClaims<LicenseInfo>, feature: Feature) -> Result<()> {
        if l.custom.features.is_none() {
            return Ok(());
        }
        let features = l.custom.features.as_ref().unwrap();
        if !features.contains(&feature.to_string()) {
            return Err(ErrorCode::LicenseKeyInvalid(format!(
                "license key does not support feature {}, supported features: {:?}",
                feature, features
            )));
        }
        Ok(())
    }
}
