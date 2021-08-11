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

use crate::crypto::{PrivKey, Signer};
use crate::types::clean_0x;
use serde::Deserialize;
use std::cell::Cell;

use std::str::FromStr;
use std::time::Duration;

const LOW_LIMIT_TIMEVAL: u64 = 600;

#[derive(Debug, Deserialize, Clone)]
pub struct PrivateKey {
    signer: PrivKey,
}

impl PrivateKey {
    pub fn new(sk: &str) -> Self {
        let signer = PrivKey::from_str(clean_0x(sk)).expect("Private key is wrong.");
        PrivateKey { signer }
    }
}

#[derive(Debug, Clone)]
pub struct BftTimer {
    // in milliseconds.
    total_duration: Cell<u64>,
    // fraction: (numerator, denominator)
    propose: (u64, u64),
    prevote: (u64, u64),
    precommit: (u64, u64),
    commit: (u64, u64),
}

impl Default for BftTimer {
    fn default() -> Self {
        BftTimer {
            total_duration: Cell::new(3000),
            propose: (24, 30),
            prevote: (6, 30),
            precommit: (6, 30),
            commit: (4, 30),
        }
    }
}

impl BftTimer {
    pub fn get_total_duration(&self) -> u64 {
        self.total_duration.get()
    }

    pub fn set_total_duration(&self, duration: u64) {
        self.total_duration.set(duration);
    }

    pub fn get_propose(&self) -> Duration {
        if self.total_duration.get() < LOW_LIMIT_TIMEVAL {
            Duration::from_millis(LOW_LIMIT_TIMEVAL)
        } else {
            Duration::from_millis(self.total_duration.get() * self.propose.0 / self.propose.1)
        }
    }

    pub fn get_prevote(&self) -> Duration {
        if self.total_duration.get() < LOW_LIMIT_TIMEVAL {
            Duration::from_millis(LOW_LIMIT_TIMEVAL)
        } else {
            Duration::from_millis(self.total_duration.get() * self.prevote.0 / self.prevote.1)
        }
    }

    // pub fn get_precommit(&self) -> Duration {
    //     if self.total_duration.get() < LOW_LIMIT_TIMEVAL {
    //         Duration::from_millis(LOW_LIMIT_TIMEVAL)
    //     } else {
    //         Duration::from_millis(self.total_duration.get() * self.precommit.0 / self.precommit.1)
    //     }
    // }

    #[allow(dead_code)]
    fn get_commit(&self) -> Duration {
        Duration::from_millis(self.total_duration.get() * self.commit.0 / self.commit.1)
    }
}

pub struct BftParams {
    pub timer: BftTimer,
    pub signer: Signer,
    pub issue_nil_block: bool,
}

impl BftParams {
    pub fn new(priv_key: &PrivateKey) -> Self {
        BftParams {
            signer: Signer::from(priv_key.signer),
            timer: BftTimer::default(),
            issue_nil_block: true,
        }
    }
}
