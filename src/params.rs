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

use std::cell::Cell;

use crate::config::BftConfig;
use cita_types::Address;
use std::str::FromStr;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct BftTimer {
    // in milliseconds.
    total_duration: Cell<u64>,
    // fraction: (numerator, denominator)
    propose: (u64, u64),
    prevote: (u64, u64),
    #[allow(dead_code)]
    precommit: (u64, u64),
    commit: (u64, u64),
    low_limit_timeval: u64,
}

impl BftTimer {
    pub fn new(config: &BftConfig) -> Self {
        Self {
            total_duration: Cell::new(config.block_interval),
            propose: (config.propose_phase, config.whole_phase),
            prevote: (config.prevote_phase, config.whole_phase),
            precommit: (config.precommit_phase, config.whole_phase),
            commit: (config.commit_phase, config.whole_phase),
            low_limit_timeval: config.low_limit_interval,
        }
    }
}

impl BftTimer {
    pub fn get_total_duration(&self) -> u64 {
        self.total_duration.get()
    }

    pub(crate) fn set_total_duration(&self, duration: u64) {
        self.total_duration.set(duration);
    }

    pub fn get_propose(&self) -> Duration {
        Duration::from_millis(self.total_duration.get() * self.propose.0 / self.propose.1)
    }

    pub fn get_prevote(&self) -> Duration {
        Duration::from_millis(self.total_duration.get() * self.prevote.0 / self.prevote.1)
    }

    // pub fn get_precommit(&self) -> Duration {
    //     if self.total_duration.get() < LOW_LIMIT_TIMEVAL {
    //         Duration::from_millis(LOW_LIMIT_TIMEVAL)
    //     } else {
    //         Duration::from_millis(self.total_duration.get() * self.precommit.0 / self.precommit.1)
    //     }
    // }

    #[allow(dead_code)]
    pub(crate) fn get_commit(&self) -> Duration {
        Duration::from_millis(self.total_duration.get() * self.commit.0 / self.commit.1)
    }
}

pub struct BftParams {
    pub timer: BftTimer,
    pub node_address: Address,
    pub issue_nil_block: bool,
    pub wal_path: String,
    pub authority_path: String,
    pub max_proposal_time_coef: u64,
    pub server_retry_interval: u64,
}

impl BftParams {
    pub fn new(config: &BftConfig) -> Self {
        BftParams {
            node_address: Address::from_str(&config.node_address).unwrap(),
            timer: BftTimer::new(config),
            issue_nil_block: config.issue_nil_block,
            wal_path: config.wal_path.clone(),
            authority_path: config.authority_path.clone(),
            max_proposal_time_coef: config.max_proposal_time_coef,
            server_retry_interval: config.server_retry_interval,
        }
    }

    pub fn set_total_duration(&mut self, duration: u64) {
        if duration == 0 {
            self.timer.set_total_duration(3000);
            self.issue_nil_block = false;
        } else if duration < self.timer.low_limit_timeval {
            self.timer.set_total_duration(self.timer.low_limit_timeval);
            self.issue_nil_block = true;
        } else {
            self.timer.set_total_duration(duration);
            self.issue_nil_block = true;
        }
    }
}
