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

use crate::crypto::{pubkey_to_address, Sign, Signature};
use crate::message::Step;
use crate::types::{Address, H256};
use bincode::serialize;
use serde::{Deserialize, Serialize};

use cita_logger::{debug, error, info, trace, warn};
use hashable::Hashable;
use lru_cache::LruCache;
use std::collections::BTreeMap;

// height -> round collector
#[derive(Debug)]
pub struct VoteCollector {
    pub votes: LruCache<u64, RoundCollector>,
}

impl VoteCollector {
    pub fn new() -> Self {
        VoteCollector {
            votes: LruCache::new(16),
        }
    }

    pub fn add(
        &mut self,
        height: u64,
        round: u64,
        step: Step,
        sender: Address,
        vote: &VoteMessage,
    ) -> bool {
        if self.votes.contains_key(&height) {
            self.votes
                .get_mut(&height)
                .unwrap()
                .add(round, step, sender, vote)
        } else {
            let mut round_votes = RoundCollector::new();
            round_votes.add(round, step, sender, vote);
            self.votes.insert(height, round_votes);
            true
        }
    }

    pub fn get_voteset(&mut self, height: u64, round: u64, step: Step) -> Option<VoteSet> {
        self.votes
            .get_mut(&height)
            .and_then(|rc| rc.get_voteset(round, step))
    }
}

//round -> step collector
#[derive(Debug)]
pub struct RoundCollector {
    pub round_votes: LruCache<u64, StepCollector>,
}

impl RoundCollector {
    pub fn new() -> Self {
        RoundCollector {
            round_votes: LruCache::new(16),
        }
    }

    pub fn add(&mut self, round: u64, step: Step, sender: Address, vote: &VoteMessage) -> bool {
        if self.round_votes.contains_key(&round) {
            self.round_votes
                .get_mut(&round)
                .unwrap()
                .add(step, sender, &vote)
        } else {
            let mut step_votes = StepCollector::new();
            step_votes.add(step, sender, &vote);
            self.round_votes.insert(round, step_votes);
            true
        }
    }

    pub fn get_voteset(&mut self, round: u64, step: Step) -> Option<VoteSet> {
        self.round_votes
            .get_mut(&round)
            .and_then(|sc| sc.get_voteset(step))
    }
}

//step -> voteset
#[derive(Debug)]
pub struct StepCollector {
    pub step_votes: BTreeMap<Step, VoteSet>,
}

impl StepCollector {
    pub fn new() -> Self {
        StepCollector {
            step_votes: BTreeMap::new(),
        }
    }

    pub fn add(&mut self, step: Step, sender: Address, vote: &VoteMessage) -> bool {
        self.step_votes
            .entry(step)
            .or_insert_with(VoteSet::new)
            .add(sender, vote)
    }

    pub fn get_voteset(&self, step: Step) -> Option<VoteSet> {
        self.step_votes.get(&step).cloned()
    }
}

//1. sender's votemessage 2. proposal'hash count
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct VoteSet {
    pub votes_by_sender: BTreeMap<Address, VoteMessage>,
    pub votes_by_proposal: BTreeMap<H256, u64>,
    pub count: u64,
}

impl VoteSet {
    pub fn new() -> Self {
        VoteSet {
            votes_by_sender: BTreeMap::new(),
            votes_by_proposal: BTreeMap::new(),
            count: 0,
        }
    }

    //just add ,not check
    pub fn add(&mut self, sender: Address, vote: &VoteMessage) -> bool {
        let mut added = false;
        self.votes_by_sender.entry(sender).or_insert_with(|| {
            added = true;
            vote.to_owned()
        });
        if added {
            self.count += 1;
            let hash = vote.proposal.unwrap_or_else(H256::default);
            *self.votes_by_proposal.entry(hash).or_insert(0) += 1;
        }
        added
    }

    pub fn check(
        &self,
        h: u64,
        r: u64,
        step: Step,
        authorities: &[Address],
    ) -> Result<Option<H256>, &str> {
        let mut votes_by_proposal: BTreeMap<H256, u64> = BTreeMap::new();
        trace!(
            "check votes_by_sender {:?} authorities{:?}",
            self.votes_by_sender,
            authorities
        );
        for (sender, vote) in &self.votes_by_sender {
            if authorities.contains(sender) {
                let msg = serialize(&(h, r, step, sender, vote.proposal)).unwrap();
                let signature = &vote.signature;
                if let Ok(pubkey) = signature.recover(&msg.crypt_hash()) {
                    if pubkey_to_address(&pubkey) == *sender {
                        let hash = vote.proposal.unwrap_or_else(H256::default);
                        // inc the count of vote for hash
                        *votes_by_proposal.entry(hash).or_insert(0) += 1;
                    }
                }
            }
        }
        trace!(" check votes get {:?}", votes_by_proposal);
        for (hash, count) in &votes_by_proposal {
            if (*count * 3) as usize > authorities.len() * 2 {
                if hash.is_zero() {
                    return Ok(None);
                } else {
                    return Ok(Some(*hash));
                }
            }
        }
        Err("vote set check error!")
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct VoteMessage {
    pub proposal: Option<H256>,
    pub signature: Signature,
}

#[derive(Debug)]
pub struct ProposalCollector {
    pub proposals: LruCache<u64, ProposalRoundCollector>,
}

impl ProposalCollector {
    pub fn new() -> Self {
        ProposalCollector {
            proposals: LruCache::new(16),
        }
    }

    pub fn add(&mut self, height: u64, round: u64, proposal: Proposal) -> bool {
        if self.proposals.contains_key(&height) {
            self.proposals
                .get_mut(&height)
                .unwrap()
                .add(round, proposal)
        } else {
            let mut round_proposals = ProposalRoundCollector::new();
            round_proposals.add(round, proposal);
            self.proposals.insert(height, round_proposals);
            true
        }
    }

    pub fn get_proposal(&mut self, height: u64, round: u64) -> Option<Proposal> {
        self.proposals
            .get_mut(&height)
            .and_then(|prc| prc.get_proposal(round))
    }
}

#[derive(Debug)]
pub struct ProposalRoundCollector {
    pub round_proposals: LruCache<u64, Proposal>,
}

impl ProposalRoundCollector {
    pub fn new() -> Self {
        ProposalRoundCollector {
            round_proposals: LruCache::new(16),
        }
    }

    pub fn add(&mut self, round: u64, proposal: Proposal) -> bool {
        if self.round_proposals.contains_key(&round) {
            false
        } else {
            self.round_proposals.insert(round, proposal);
            true
        }
    }

    pub fn get_proposal(&mut self, round: u64) -> Option<Proposal> {
        self.round_proposals.get_mut(&round).cloned()
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Proposal {
    pub phash: H256,
    pub lock_round: Option<u64>,
    pub lock_votes: Option<VoteSet>,
}

impl Proposal {
    pub fn new(phash: H256) -> Self {
        Self {
            phash,
            lock_round: None,
            lock_votes: None,
        }
    }
    pub fn is_default(&self) -> bool {
        self.phash.is_zero()
    }

    pub fn check(&self, h: u64, authorities: &[Address]) -> bool {
        if self.lock_round.is_none() && self.lock_votes.is_none() {
            true
        } else {
            let round = self.lock_round.unwrap();

            let ret = self
                .lock_votes
                .as_ref()
                .unwrap()
                .check(h, round, Step::Prevote, authorities);

            match ret {
                Ok(Some(p)) => p == self.phash,
                _ => false,
            }
        }
    }
}
