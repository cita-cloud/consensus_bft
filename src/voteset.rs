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

use crate::message::{SignedFollowerVote, Step};
use crate::util::recover_sig;
use cita_types::{Address, H256};
use log::trace;
use lru_cache::LruCache;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

// height -> round collector
#[derive(Debug)]
pub struct VoteCollector {
    pub votes: LruCache<u64, RoundCollector>,
}

impl Default for VoteCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl VoteCollector {
    pub fn new() -> Self {
        VoteCollector {
            votes: LruCache::new(16),
        }
    }

    pub fn add(&mut self, sender: Address, sign_vote: &SignedFollowerVote) -> bool {
        let height = sign_vote.vote.height;
        if self.votes.contains_key(&height) {
            self.votes.get_mut(&height).unwrap().add(sender, sign_vote)
        } else {
            let mut round_votes = RoundCollector::new();
            round_votes.add(sender, sign_vote);
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

impl Default for RoundCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl RoundCollector {
    pub fn new() -> Self {
        RoundCollector {
            round_votes: LruCache::new(16),
        }
    }

    pub fn add(&mut self, sender: Address, sign_vote: &SignedFollowerVote) -> bool {
        let round = sign_vote.vote.round;
        let _step = sign_vote.vote.step;

        if self.round_votes.contains_key(&round) {
            self.round_votes
                .get_mut(&round)
                .unwrap()
                .add(sender, sign_vote)
        } else {
            let mut step_votes = StepCollector::new();
            step_votes.add(sender, sign_vote);
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

impl Default for StepCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl StepCollector {
    pub fn new() -> Self {
        StepCollector {
            step_votes: BTreeMap::new(),
        }
    }

    pub fn add(&mut self, sender: Address, sign_vote: &SignedFollowerVote) -> bool {
        // change Step::NewViewRes -> Step::NewView
        let step = if sign_vote.vote.step == Step::NewViewRes {
            Step::NewView
        } else {
            sign_vote.vote.step
        };
        self.step_votes
            .entry(step)
            .or_insert_with(VoteSet::new)
            .add(sender, sign_vote)
    }

    pub fn get_voteset(&self, step: Step) -> Option<VoteSet> {
        self.step_votes.get(&step).cloned()
    }
}

//1. sender's votemessage 2. proposal'hash count
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct VoteSet {
    pub votes_by_sender: BTreeMap<Address, SignedFollowerVote>,
    pub votes_by_proposal: BTreeMap<H256, u64>,
    pub count: u64,
}

impl VoteSet {
    pub fn new() -> Self {
        VoteSet::default()
    }

    //just add ,not check
    pub fn add(&mut self, sender: Address, sign_vote: &SignedFollowerVote) -> bool {
        let mut added = false;
        self.votes_by_sender.entry(sender).or_insert_with(|| {
            added = true;
            sign_vote.to_owned()
        });
        if added {
            self.count += 1;
            let hash = sign_vote.vote.hash.unwrap_or_default();
            *self.votes_by_proposal.entry(hash).or_insert(0) += 1;
        }
        added
    }

    pub fn check(
        &self,
        _h: u64,
        _r: u64,
        _step: Step,
        authorities: &[Address],
    ) -> Result<Option<H256>, &str> {
        let mut votes_by_proposal: BTreeMap<H256, u64> = BTreeMap::new();
        trace!(
            "check votes_by_sender {:?} authorities{:?}",
            self.votes_by_sender,
            authorities
        );
        for (sender, sign_vote) in &self.votes_by_sender {
            if authorities.contains(sender) {
                let msg = Vec::from(&sign_vote.vote);

                if recover_sig(&sign_vote.sig, &msg).as_slice() == sender.0 {
                    let hash = sign_vote.vote.hash.unwrap_or_default();
                    // inc the count of vote for hash
                    *votes_by_proposal.entry(hash).or_insert(0) += 1;
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

// #[derive(Serialize, Deserialize, Clone, Debug)]
// pub struct SignedFollowerVote {
//     pub proposal: Option<H256>,
//     pub signature: Vec<u8>,
// }

#[derive(Debug)]
pub struct ProposalCollector {
    pub proposals: LruCache<u64, ProposalRoundCollector>,
}

impl Default for ProposalCollector {
    fn default() -> Self {
        Self::new()
    }
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

impl Default for ProposalRoundCollector {
    fn default() -> Self {
        Self::new()
    }
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
