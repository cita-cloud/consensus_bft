// Copyright Rivtower Technologies LLC.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use serde_derive::Deserialize;
use toml;

#[derive(Debug, Deserialize, Clone)]
pub struct BftConfig {
    pub network_port: u16,
    pub controller_port: u16,
    pub node_id: u64,
}

impl BftConfig {
    pub fn new(config_str: &str) -> Self {
        toml::from_str::<BftConfig>(config_str).expect("Error while parsing config")
    }
}

#[cfg(test)]
mod tests {
    use super::BftConfig;

    #[test]
    fn test_read_bft_config() {
        {
            let toml_str = r#"
            network_port = 50000
            controller_port = 50005
            "#;

            let config = BftConfig::new(toml_str);
            assert_eq!(config.network_port, 50000);
            assert_eq!(config.controller_port, 50005);
        }
        {
            // raft-rs treats 0 as invalid id,
            // but we allow it here, and +1 when pass it to raft.
            let toml_str = r#"
            network_port = 50000
            controller_port = 50005
            "#;

            let config = BftConfig::new(toml_str);
            assert_eq!(config.network_port, 50000);
            assert_eq!(config.controller_port, 50005);
        }
    }
}
