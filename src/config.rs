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

use std::fs;

use cloud_util::common::read_toml;
use serde_derive::Deserialize;

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct BftConfig {
    pub network_port: u16,

    pub consensus_port: u16,

    pub controller_port: u16,

    pub crypto_port: u16,
    /// set whether send no-tx block, default true
    pub issue_nil_block: bool,

    pub node_address: String,

    pub block_interval: u64,

    pub whole_phase: u64,

    pub propose_phase: u64,

    pub prevote_phase: u64,

    pub precommit_phase: u64,

    pub commit_phase: u64,

    pub low_limit_interval: u64,

    pub server_retry_interval: u64,

    pub wal_path: String,

    pub authority_path: String,

    pub max_proposal_time_coef: u64,

    pub enable_metrics: bool,

    pub metrics_port: u16,

    pub metrics_buckets: Vec<f64>,
}

impl Default for BftConfig {
    fn default() -> Self {
        Self {
            network_port: 50000,
            consensus_port: 50001,
            controller_port: 50004,
            crypto_port: 50005,
            issue_nil_block: true,
            node_address: "".to_string(),
            block_interval: 3000,
            whole_phase: 30,
            propose_phase: 24,
            prevote_phase: 6,
            precommit_phase: 6,
            commit_phase: 4,
            low_limit_interval: 600,
            server_retry_interval: 2,
            wal_path: "./data/wal".to_string(),
            authority_path: "./data/authorities".to_string(),
            max_proposal_time_coef: 8,
            enable_metrics: true,
            metrics_port: 60001,
            metrics_buckets: vec![
                0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0, 25.0, 50.0, 75.0, 100.0, 250.0, 500.0,
            ],
        }
    }
}

impl BftConfig {
    pub fn new(config_str: &str) -> Self {
        let mut config: BftConfig = read_toml(config_str, "consensus_bft");
        let node_address_path = config.node_address.clone();
        config.node_address = fs::read_to_string(node_address_path).unwrap();
        config
    }
}

#[cfg(test)]
mod tests {
    use super::BftConfig;

    #[test]
    fn basic_test() {
        let config = BftConfig::new("example/config.toml");

        assert_eq!(config.network_port, 50000);
        assert_eq!(config.consensus_port, 50001);
        assert_eq!(config.controller_port, 50004);
        assert_eq!(config.crypto_port, 50005);
        assert_eq!(config.controller_port, 50004);
        assert_eq!(
            config.node_address,
            "c356876e7f4831476f99ea0593b0cd7a6053e4d3".to_string()
        );
    }
}
