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

use crate::cita_bft::Step;
use crate::crypto::{pubkey_to_address, Sign, Signature};
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
    pub votes: LruCache<usize, RoundCollector>,
}

impl VoteCollector {
    pub fn new() -> Self {
        VoteCollector {
            votes: LruCache::new(16),
        }
    }

    pub fn add(
        &mut self,
        height: usize,
        round: usize,
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

    pub fn get_voteset(&mut self, height: usize, round: usize, step: Step) -> Option<VoteSet> {
        self.votes
            .get_mut(&height)
            .and_then(|rc| rc.get_voteset(round, step))
    }
}

//round -> step collector
#[derive(Debug)]
pub struct RoundCollector {
    pub round_votes: LruCache<usize, StepCollector>,
}

impl RoundCollector {
    pub fn new() -> Self {
        RoundCollector {
            round_votes: LruCache::new(16),
        }
    }

    pub fn add(&mut self, round: usize, step: Step, sender: Address, vote: &VoteMessage) -> bool {
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

    pub fn get_voteset(&mut self, round: usize, step: Step) -> Option<VoteSet> {
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
    pub votes_by_proposal: BTreeMap<H256, usize>,
    pub count: usize,
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
        h: usize,
        r: usize,
        step: Step,
        authorities: &[Address],
    ) -> Result<Option<H256>, &str> {
        let mut votes_by_proposal: BTreeMap<H256, usize> = BTreeMap::new();
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
            if *count * 3 > authorities.len() * 2 {
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
    pub proposals: LruCache<usize, ProposalRoundCollector>,
}

impl ProposalCollector {
    pub fn new() -> Self {
        ProposalCollector {
            proposals: LruCache::new(16),
        }
    }

    pub fn add(&mut self, height: usize, round: usize, proposal: Proposal) -> bool {
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

    pub fn get_proposal(&mut self, height: usize, round: usize) -> Option<Proposal> {
        self.proposals
            .get_mut(&height)
            .and_then(|prc| prc.get_proposal(round))
    }
}

#[derive(Debug)]
pub struct ProposalRoundCollector {
    pub round_proposals: LruCache<usize, Proposal>,
}

impl ProposalRoundCollector {
    pub fn new() -> Self {
        ProposalRoundCollector {
            round_proposals: LruCache::new(16),
        }
    }

    pub fn add(&mut self, round: usize, proposal: Proposal) -> bool {
        if self.round_proposals.contains_key(&round) {
            false
        } else {
            self.round_proposals.insert(round, proposal);
            true
        }
    }

    pub fn get_proposal(&mut self, round: usize) -> Option<Proposal> {
        self.round_proposals.get_mut(&round).cloned()
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Proposal {
    pub phash: H256,
    pub lock_round: Option<usize>,
    pub lock_votes: Option<VoteSet>,
}

impl Proposal {
    pub fn is_default(&self) -> bool {
        self.phash.is_zero()
    }

    pub fn check(&self, h: usize, authorities: &[Address]) -> bool {
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
                Ok(Some(p)) => {
                    p == self.phash
                }
                _ => false,
            }
        }
    }
}
