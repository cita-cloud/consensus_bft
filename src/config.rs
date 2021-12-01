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

use cloud_util::common::read_toml;
use serde_derive::Deserialize;

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct BftConfig {
    pub network_port: u16,

    pub consensus_port: u16,

    pub controller_port: u16,

    pub kms_port: u16,
    /// set whether send no-tx block, default true
    pub issue_nil_block: bool,

    pub key_id: u64,

    pub node_address: String,

    pub block_interval: u64,

    pub whole_phase: u64,

    pub propose_phase: u64,

    pub prevote_phase: u64,

    pub precommit_phase: u64,

    pub commit_phase: u64,

    pub low_limit_interval: u64,

    pub server_retry_interval: u64,

    pub log_file: String,

    pub wal_path: String,

    pub authority_path: String,

    pub max_proposal_time_coef: u64,
}

impl Default for BftConfig {
    fn default() -> Self {
        Self {
            network_port: 50000,
            consensus_port: 50001,
            controller_port: 50004,
            kms_port: 50005,
            key_id: 1,
            issue_nil_block: true,
            node_address: "".to_string(),
            block_interval: 3000,
            whole_phase: 30,
            propose_phase: 24,
            prevote_phase: 6,
            precommit_phase: 6,
            commit_phase: 4,
            low_limit_interval: 600,
            server_retry_interval: 3,
            log_file: "consensus-log4rs.yaml".to_string(),
            wal_path: "./data/wal".to_string(),
            authority_path: "./data/authorities".to_string(),
            max_proposal_time_coef: 8,
        }
    }
}

impl BftConfig {
    pub fn new(config_str: &str) -> Self {
        read_toml(config_str, "consensus_bft")
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
