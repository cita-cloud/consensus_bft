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

use authority_manage::AuthorityManage;
use bincode::{deserialize, serialize};
use clap::lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::convert::{From, Into, TryInto};

use crate::params::BftParams;
use crate::voteset::{Proposal, ProposalCollector, VoteCollector, VoteSet};

use crate::votetime::TimeoutInfo;
use crate::wal::{LogType, Wal};
use cita_logger::{debug, error, info, trace, warn};

use crate::crypto::{pubkey_to_address, CreateKey, Sign, Signature, SIGNATURE_BYTES_LEN};
use crate::message::{
    BftSvrMsg, BftToCtlMsg, CtlBackBftMsg, FollowerVote, LeaderVote, NetworkProposal,
    SignedFollowerVote, SignedNetworkProposal, Step, Vote, VoteMsgType,
};
use crate::types::{Address, H256};
use cita_cloud_proto::common::{Proposal as ProtoProposal, ProposalWithProof};
use cita_cloud_proto::network::NetworkMsg;
use cita_directories::DataPath;
use engine::{unix_now, AsMillis, EngineError, Mismatch};
use hashable::Hashable;
use proof::BftProof;
use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};
use std::{cell::RefCell, fs, process::exit};
use tokio::sync::mpsc;
// #[macro_use]
// use lazy_static;

const INIT_HEIGHT: u64 = 1;
const INIT_ROUND: u64 = 0;

const MAX_PROPOSAL_TIME_COEF: u64 = 18;

const TIMEOUT_RETRANSE_MULTIPLE: u32 = 15;
const TIMEOUT_LOW_ROUND_FEED_MULTIPLE: u32 = 23;

//const BLOCK_TIMESTAMP_INTERVAL: u64 = 100;
const DEFAULT_TIME_INTERVAL: u64 = 3000;
const TIMESTAMP_JUDGE_BLOCK_INTERVAL: u64 = 200;
const TIMESTAMP_DIFF_MAX_INTERVAL: u64 = 3000;

pub type TransType = (String, Vec<u8>);
pub type PubType = (String, Vec<u8>);
type NilRound = (u64, u64);

#[derive(Debug, Clone, Copy, PartialEq)]
enum VerifiedProposalStatus {
    Ok,
    Err,
    Init,
}

impl VerifiedProposalStatus {
    pub fn value(self) -> i8 {
        match self {
            VerifiedProposalStatus::Ok => 1,
            VerifiedProposalStatus::Err => -1,
            VerifiedProposalStatus::Init => 0,
        }
    }

    pub fn is_ok(self) -> bool {
        match self {
            VerifiedProposalStatus::Ok => true,
            _ => false,
        }
    }

    pub fn is_init(self) -> bool {
        match self {
            VerifiedProposalStatus::Init => true,
            _ => false,
        }
    }
}

impl From<i8> for VerifiedProposalStatus {
    fn from(s: i8) -> Self {
        match s {
            1 => VerifiedProposalStatus::Ok,
            -1 => VerifiedProposalStatus::Err,
            0 => VerifiedProposalStatus::Init,
            _ => panic!("Invalid VerifiedProposalStatus."),
        }
    }
}

pub struct BftChannls {
    // to bft service server
    pub to_bft_rx: mpsc::UnboundedReceiver<BftSvrMsg>,
    // from bft to controller server's reqeust
    pub to_ctl_tx: mpsc::UnboundedSender<BftToCtlMsg>,
    // from bft to controller server's respons
    pub ctl_back_rx: mpsc::UnboundedReceiver<CtlBackBftMsg>,
    // from bft to netowrk quest
    pub to_net_tx: mpsc::UnboundedSender<NetworkMsg>,
    // network back msg
    pub net_back_rx: mpsc::UnboundedReceiver<NetworkMsg>,

    pub to_timer_tx: mpsc::UnboundedSender<TimeoutInfo>,

    pub timer_back_rx: mpsc::UnboundedReceiver<TimeoutInfo>,
}
pub struct Bft {
    params: BftParams,
    height: u64,
    round: u64,

    // NilRound.0: nil round 1. last nil propose round
    nil_round: NilRound,
    step: Step,
    // proof: BTreeMap<u64, BftProof>,
    hash_proposals: lru_cache::LruCache<H256, (Vec<u8>, VerifiedProposalStatus)>,
    // Timestamps of old blocks
    height_timestamps: BTreeMap<u64, u64>,
    votes: VoteCollector,
    proposals: ProposalCollector,
    proposal: Option<H256>,
    self_proposal: Option<H256>,
    lock_round: Option<u64>,
    // locked_vote: Option<VoteSet>,
    // lock_round set, locked block means itself,else means proposal's block
    // locked_block: Option<Vec<u8>>,
    wal_log: RefCell<Wal>,
    send_filter: BTreeMap<u64, Instant>,
    last_commit_round: Option<u64>,
    start_time: Duration,
    auth_manage: AuthorityManage,
    is_consensus_node: bool,
    //params meaning: key :index 0->height,1->round ,value:0->verified msg,1->verified result
    //unverified_msg: BTreeMap<(u64, u64), (Vec<u8>, VerifiedProposalStatus)>,
    // VecDeque might work, Almost always it is better to use Vec or VecDeque instead of LinkedList
    block_txs: BTreeMap<u64, Vec<u8>>,
    block_proof: Option<(u64, ProposalWithProof)>,

    // The verified blocks with the bodies of transactions.
    verified_blocks: Vec<(H256, Vec<u8>)>,

    // whether the datas above have been cleared.
    is_cleared: bool,

    leader_origins: BTreeMap<(u64, u64), u32>,

    version: Option<u32>,

    bft_channels: BftChannls,

    #[cfg(feature = "timestamp_test")]
    pub mock_time_modify: TimeOffset,
}

impl ::std::fmt::Debug for Bft {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        write!(
            f,
            "Bft {{ \
             h: {}, r: {}, nr: {:?}, s: {}, v: {:?} \
             , proposal: {:?}, \
             lock_round: {:?}, last_commit_round: {:?}, \
             is_consensus_node: {:?}, is_cleared: {} \
             }}",
            self.height,
            self.round,
            self.nil_round,
            self.step,
            self.version,
            self.proposal,
            self.lock_round,
            self.last_commit_round,
            self.is_consensus_node,
            self.is_cleared,
        )
    }
}

impl ::std::fmt::Display for Bft {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        write!(
            f,
            "Bft {{ h: {}, r: {}, s: {}, nr: {} }}",
            self.height, self.round, self.step, self.nil_round.0,
        )
    }
}

impl Bft {
    pub fn new(
        // s: Sender<PubType>,
        // ts: Sender<TimeoutInfo>,
        // r: Receiver<BftTurn>,
        params: BftParams,
        bft_channels: BftChannls,
    ) -> Bft {
        let logpath = DataPath::wal_path();
        info!("start cita-bft log {}", logpath);
        Bft {
            // pub_sender: s,
            // timer_seter: ts,
            // receiver: r,
            params,
            height: 0,
            round: INIT_ROUND,
            nil_round: (INIT_ROUND, INIT_ROUND),
            step: Step::Propose,
            hash_proposals: lru_cache::LruCache::new(100),
            height_timestamps: BTreeMap::new(),
            votes: VoteCollector::new(),
            proposals: ProposalCollector::new(),
            proposal: None,
            self_proposal: None,
            lock_round: None,
            wal_log: RefCell::new(Wal::create(&*logpath).unwrap()),
            send_filter: BTreeMap::new(),
            last_commit_round: None,
            start_time: unix_now(),
            auth_manage: AuthorityManage::new(),
            is_consensus_node: false,
            leader_origins: BTreeMap::new(),
            block_txs: BTreeMap::new(),
            block_proof: None,
            verified_blocks: Vec::new(),
            is_cleared: false,
            version: None,
            bft_channels,
        }
    }

    fn send_raw_net_msg<T: Into<Vec<u8>>>(&self, rtype: &str, origin: u64, msg: T) {
        let net_msg = NetworkMsg {
            module: "consensus".to_owned(),
            r#type: rtype.to_string(),
            origin,
            msg: msg.into(),
        };
        self.bft_channels.to_net_tx.send(net_msg).unwrap();
    }

    fn is_only_one_node(&self) -> bool {
        let len = self.auth_manage.validator_n();
        if len == 1 {
            return true;
        }
        false
    }

    fn is_round_leader(
        &self,
        height: u64,
        round: u64,
        address: &Address,
    ) -> Result<bool, EngineError> {
        let p = &self.auth_manage;
        if p.validators.is_empty() {
            warn!("There are no authorities");
            return Err(EngineError::NotAuthorized(Address::zero()));
        }
        let proposer_nonce = height + round;
        let proposer: &Address = p
            .validators
            .get(proposer_nonce as usize % p.validators.len())
            .expect(
                "There are validator_n() authorities; \
                 taking number modulo validator_n() gives number in validator_n() range; qed",
            );
        if proposer == address {
            Ok(true)
        } else {
            //info!(" This round  proposer {:?} comming {:?}", proposer, address);
            Ok(false)
        }
    }

    pub fn pub_block(&self, block: &ProposalWithProof) {}

    fn get_verified_block(&self, hash: &H256) -> Option<Vec<u8>> {
        for (vhash, block) in &self.verified_blocks {
            if vhash == hash {
                return Some(block.clone());
            }
        }
        None
    }

    fn remove_verified_block(&mut self, height: u64) {
        // self.verified_blocks
        //     .retain(|(_, block)| block.get_header().get_height() as u64 > height);
    }

    pub fn pub_proposal(&mut self, compact_signed_proposal: &SignedNetworkProposal) {
        self.send_raw_net_msg(
            VoteMsgType::Proposal.into(),
            0,
            compact_signed_proposal.clone(),
        );
    }

    fn follower_proc_prevote(&mut self, height: u64, round: u64, hash: H256) -> bool {
        trace!("Follower proc prevote hash {:?} self {:?}", hash, self);
        let old_lock_round = self.unlock_polc(round);
        if !hash.is_zero() {
            // Try effort to find real proposal block
            if self.proposal != Some(hash) {
                warn!(
                    "Follower proc prevote proposal {:?} not equal hash {:?} lock_round {:?}",
                    self.proposal, hash, old_lock_round
                );
                if self.search_history_proposal(hash) {
                    self.proposal = Some(hash);
                }
            }

            if self.proposal == Some(hash) {
                debug!("Follower proc prevoted proposal {:?} ", self.proposal);
                self.lock_round = Some(round);

                if self.pre_proc_precommit() {
                    self.change_state_step(height, round, Step::PrecommitWait);
                    self.wal_save_state_step(height, round, Step::PrecommitWait);
                    self.set_state_timeout(
                        height,
                        round,
                        Step::PrecommitWait,
                        self.proposal_interval_round_multiple(round),
                    );

                    if let Some(hash) = self.check_saved_vote(height, round, Step::Precommit) {
                        self.follower_proc_precommit(height, round, Some(hash));
                    }
                }
                return true;
            }
        }
        //self.pub_and_broadcast_message(height, round, Step::Precommit, Some(H256::default()));
        self.new_round_start(height, round + 1);
        false
    }

    fn check_vote_over_period(&self, height: u64, round: u64, check_step: Step) -> bool {
        // When round > self.round pass
        if height < self.height
            || (height == self.height && round < self.round)
            || (height == self.height
                && self.round == round
                && (self.step != Step::NewView && self.step > check_step))
        {
            return true;
        }
        false
    }

    fn _is_right_step(&self, height: u64, round: u64, step: Step) -> bool {
        self.round == round && self.step == step && self.height == height
    }

    fn unlock_polc(&mut self, round: u64) -> Option<u64> {
        let old = self.lock_round;
        if self.lock_round.is_some() && self.lock_round.unwrap() < round {
            //&& round <= self.round {
            // We see new lock block unlock my old locked block
            trace!(
                " Now {} unlock locked block round {:?} ",
                self,
                self.lock_round
            );
            self.lock_round = None;
            //self.locked_vote = None;
        }
        old
    }

    fn proc_new_view(&mut self, height: u64, round: u64) -> bool {
        debug!(
            "Uniform proc new view {} begin h: {}, r: {}",
            self, height, round
        );
        // This tobe considered the step
        if self.check_vote_over_period(height, round, Step::PrecommitWait) {
            return false;
        }

        let vote_set = self.votes.get_voteset(height, round, Step::NewView);
        trace!("Uniform proc newview {} vote_set: {:?}", self, vote_set);
        let vlen = vote_set.as_ref().map(|v| v.count).unwrap_or(0);

        if vlen > 0 && self.is_equal_threshold(vlen) {
            self.pub_newview_message(height, round);
            self.new_round_start(height, round + 1);
            return true;
        } else if self.is_only_one_node() {
            self.new_round_start(height, round + 1);
        }
        false
    }

    fn search_history_proposal(&mut self, checked_hash: H256) -> bool {
        self.hash_proposals.contains_key(&checked_hash)

        // if let Some(proposal) = self.proposals.get_proposal(height, round) {
        //     if let Ok(block) = pro_block {
        //         let bhash: H256 = block.crypt_hash();
        //         if bhash == checked_hash {
        //             self.set_proposal_and_block(Some(bhash), Some(block));
        //             return true;
        //         } else {
        //             warn!(
        //                 "History proposal not find queal to prevote's proposal,
        //             block proposal {:?}, checked hash {:?}",
        //                 bhash, checked_hash
        //             );
        //         }
        //     } else {
        //         error!(
        //             "Compact block can't format from block h {},r {}",
        //             height, round
        //         );
        //     }
        // }

        // false
    }

    fn leader_proc_prevote(&mut self, height: u64, round: u64, _hash: Option<H256>) -> bool {
        debug!(
            "leader_proc_prevote h {} r{} hash {:?}",
            height, round, _hash
        );

        if self.check_vote_over_period(height, round, Step::PrevoteWait) {
            return false;
        }

        let vote_set = self.votes.get_voteset(height, round, Step::Prevote);
        trace!("Leader proc prevote {} vote_set: {:?}", self, vote_set);
        if let Some(vote_set) = vote_set {
            if self.is_above_threshold(vote_set.count) {
                let mut vote_one_flag = false;
                let mut next_flag = false;

                for (hash, count) in &vote_set.votes_by_proposal {
                    if self.is_above_threshold(*count) {
                        // if has locked block,and now get polc, then unlock
                        let old_lock_round = self.unlock_polc(round);

                        if hash.is_zero() {
                            self.clean_proposal_locked_info();
                            next_flag = true;
                        } else {
                            //let fvote = FollowerVote
                            if self.proposal != Some(*hash) {
                                warn!(
                                    "Leader proc prevote proposal {:?} not equal hash {:?} lock_round {:?}",
                                    self.proposal,hash,old_lock_round
                                );
                                if self.search_history_proposal(*hash) {
                                    self.proposal = Some(*hash);
                                }
                            }
                            if self.proposal == Some(*hash) {
                                debug!("Leader proc prevote proposal {:?}", self.proposal);
                                // Here means has locked in earlier round
                                self.lock_round = Some(round);
                                vote_one_flag = true;
                            } else {
                                next_flag = true;
                            }
                        }
                        //more than one hash have threahold is wrong !! do some check ??
                        break;
                    }
                }
                if vote_one_flag {
                    //to be fix
                    // let _ = self.wal_save_message(height, LogType::QuorumVotes, &vmsg);
                    self.pub_leader_message(height, round, Step::Prevote, self.proposal);
                    self.add_leader_self_vote(height, round, Step::Precommit, self.proposal);
                    //self.add_leader_self_vote(height, round, Step::Prevote, self.proposal);
                    self.change_state_step(height, round, Step::PrecommitWait);
                    self.wal_save_state_step(height, round, Step::PrecommitWait);
                    // Give same timeout as proposal,wait for follower's precommit comming
                    self.set_state_timeout(
                        height,
                        round,
                        Step::PrecommitWait,
                        self.proposal_interval_round_multiple(round),
                    );
                }
                // All nodes votes,and not get enough vote, enter next round,not wait for proposal_wait reach
                else if next_flag || self.is_all_vote(vote_set.count) {
                    trace!(
                        "leader proc prevote errflag {} vote num{}",
                        next_flag,
                        vote_set.count
                    );
                    self.pub_leader_message(height, round, Step::Prevote, None);
                    self.new_round_start(height, round + 1);
                }
            } else if self.is_equal_threshold(vote_set.count) && round > self.round {
                trace!(
                    "future prevote leader proc prevote round {} vote num{}",
                    round,
                    vote_set.count
                );
                // when leader's role circulate back to this node, but this node delayed,not vote
                // maybe votes maybe never
                self.add_leader_self_vote(height, round, Step::Prevote, Some(H256::default()));
                self.pub_leader_message(height, round, Step::Prevote, None);
                self.new_round_start(height, round + 1);
            }
            return true;
        }
        false
    }

    fn leader_proc_precommit(&mut self, height: u64, round: u64, _vote_hash: Option<H256>) -> bool {
        debug!(
            "leader proc precommit {} begin h: {}, r: {}",
            self, height, round
        );
        if self.check_vote_over_period(height, round, Step::PrecommitWait) {
            return false;
        }

        let vote_set = self.votes.get_voteset(height, round, Step::Precommit);
        trace!(
            "leader proc precommit {} deal h: {}, r: {}, vote_set: {:?}",
            self,
            height,
            round,
            vote_set
        );
        if let Some(vote_set) = vote_set {
            if self.is_above_threshold(vote_set.count) {
                trace!(
                    "leader proc precommit {} is_above_threshold h: {}, r: {}",
                    self,
                    height,
                    round
                );

                for (hash, count) in &vote_set.votes_by_proposal {
                    if self.is_above_threshold(*count) {
                        trace!(
                            "leader proc precommit {} is_above_threshold hash: {:?}, count: {}",
                            self,
                            hash,
                            count
                        );
                        if hash.is_zero() {
                            trace!("leader proc precommit is zero");
                            self.pub_leader_message(height, round, Step::Precommit, None);
                            self.new_round_start(height, round + 1);
                        } else if self.proposal == Some(*hash) {
                            self.last_commit_round = Some(round);
                            trace!("leader proc precommit is hash {:?}", hash);
                            self.pub_leader_message(height, round, Step::Precommit, Some(*hash));
                            self.do_commit_work(height, round);
                        } else {
                            warn!(
                                "leader proc precommit  proposal {:?} is not equal hash {:?}",
                                self.proposal, hash
                            );
                            self.pub_leader_message(height, round, Step::Precommit, None);
                            self.new_round_start(height, round + 1);
                        }
                        break;
                    }
                }
                return true;
            }
        }
        false
    }

    fn get_faulty_limit_number(&self) -> u64 {
        self.auth_manage.validator_n() as u64 / 3
    }

    fn is_above_threshold(&self, n: u64) -> bool {
        n * 3 > self.auth_manage.validator_n() as u64 * 2
    }

    fn is_above_threshold_old(&self, n: u64) -> bool {
        n * 3 > self.auth_manage.validators_old.len() as u64 * 2
    }

    fn is_equal_threshold(&self, n: u64) -> bool {
        n == (self.auth_manage.validator_n() as u64 * 2) / 3
    }

    fn is_all_vote(&self, n: u64) -> bool {
        n == self.auth_manage.validator_n() as u64
    }

    fn pre_proc_precommit(&mut self) -> bool {
        let height = self.height;
        let round = self.round;
        let mut lock_ok = false;

        trace!("pre proc precommit begin {}", self);

        let lock_ok = self.lock_round.map_or(false, |lround| lround == round);
        if lock_ok {
            self.pub_follower_message(height, round, Step::Precommit, self.proposal);
        } else {
            self.pub_follower_message(height, round, Step::Precommit, Some(H256::default()));
        }
        true
    }

    fn check_saved_vote(&mut self, height: u64, round: u64, step: Step) -> Option<H256> {
        let vote_set = self.votes.get_voteset(height, round, step);
        trace!(
            "follower check saved vote {} deal h: {}, r: {} s {}, vote_set: {:?}",
            self,
            height,
            round,
            step,
            vote_set
        );
        if let Some(vote_set) = vote_set {
            if self.is_above_threshold(vote_set.count) {
                trace!(
                    "follower check saved vote {} is_above_threshold h: {}, r: {}",
                    self,
                    height,
                    round
                );

                for (hash, count) in vote_set.votes_by_proposal {
                    if self.is_above_threshold(count) {
                        trace!(
                            "check saved vote {} above threshold hash: {:?}, count: {}",
                            self,
                            hash,
                            count
                        );
                        return Some(hash);
                    }
                }
                return Some(H256::default());
            } else {
                trace!(
                    "check voteset count is not to threshold {} self {}",
                    self,
                    vote_set.count
                );
            }
        }
        None
    }

    fn do_commit_work(&mut self, height: u64, round: u64) {
        if self.check_and_commit_work(height, round) {
            /*wait for new status*/
            self.wal_save_state_step(height, round, Step::Commit);
            self.change_state_step(height, round, Step::Commit);
            self.set_state_timeout(
                height,
                round,
                Step::Commit,
                Duration::from_millis(DEFAULT_TIME_INTERVAL)
                    * ((round + 1) * self.auth_manage.validator_n() as u64) as u32,
            );
        } else {
            // clean the param if not locked
            if self.lock_round.is_none() {
                self.clean_proposal_locked_info();
            }
            self.new_round_start(height, round + 1);
        }
    }

    fn follower_proc_precommit(&mut self, height: u64, round: u64, hash: Option<H256>) -> bool {
        debug!(
            "follower proc precommit {} begin h: {}, r: {} hash {:?}",
            self, height, round, hash
        );
        if self.check_vote_over_period(height, round, Step::PrecommitWait) {
            return false;
        }

        if let Some(hash) = hash {
            if !hash.is_zero() {
                if self.proposal != Some(hash) && self.search_history_proposal(hash) {
                    self.proposal = Some(hash);
                }
                if self.proposal == Some(hash) {
                    self.last_commit_round = Some(round);
                    if let Some(vote_set) = self.votes.get_voteset(height, round, Step::Precommit) {
                        let lv = Self::collect_votes(&vote_set, hash);
                        if !self.is_above_threshold(lv.len() as u64) {
                            warn!(" precommit vot not above threshold hash {:?}", hash);
                            return false;
                        }

                        let vmsg: Vec<u8> = LeaderVote {
                            height,
                            round,
                            hash: Some(hash),
                            votes: lv,
                        }
                        .into();

                        let _ = self.wal_save_message(height, LogType::QuorumVotes, &vmsg);
                        //self.wal_save_state_step(height, round, Step::PrecommitWait);
                    }
                    self.do_commit_work(height, round);
                } else {
                    //self.clean_proposal_locked_info();
                    // Wait for syncing
                    warn!(" Saved proposal not equal to hash");
                }
                return true;
            }
        }
        self.new_round_start(height, round + 1);
        false
    }

    fn check_and_commit_work(&mut self, height: u64, round: u64) -> bool {
        trace!(
            "uniform check commit work {} begin h: {}, r: {}, last_commit_round: {:?}",
            self,
            height,
            round,
            self.last_commit_round
        );
        if self.height == height && self.round == round {
            if let Some(cround) = self.last_commit_round {
                if cround == round && self.proposal.is_some() {
                    return self.commit_block(height, round);
                }
            }
        }
        trace!("uniform check and commit_work failed");
        false
    }

    fn deal_old_height_when_commited(&mut self, height: u64) -> bool {
        debug!("deal old_height when commited {} h: {} ", self, height);
        if self.height <= height {
            self.clean_proposal_locked_info();
            self.clean_filter_info();
            self.clean_block_txs();
            self.clean_proof_hash();
            self.clean_leader_origins();
            self.remove_verified_block(height);
            self.clean_height_timestamps();
            self.nil_round = (INIT_ROUND, INIT_ROUND);
            return true;
        }
        false
    }

    fn generate_proof(&mut self, height: u64, round: u64, hash: H256) -> Option<Vec<u8>> {
        let vote_set = self.votes.get_voteset(height, round, Step::Precommit);
        if let Some(vote_set) = vote_set {
            let votes = Self::collect_votes(&vote_set, hash);
            if self.is_above_threshold(votes.len() as u64) {
                let lv = LeaderVote {
                    height,
                    round,
                    hash: Some(hash),
                    votes,
                };
                return Some(lv.into());
            }
        }
        None
    }

    fn commit_block(&mut self, height: u64, commit_round: u64) -> bool {
        trace!("commit_block {:?} start, round {}", self, commit_round);
        if let Some(hash) = self.proposal {
            let res = self.hash_proposals.get_mut(&hash).cloned();
            if let Some((raw_proposal, _)) = res {
                let proof = self.generate_proof(height, commit_round, hash);
                if let Some(proof) = proof {
                    let pproof = ProposalWithProof {
                        proposal: Some(ProtoProposal {
                            height,
                            data: raw_proposal.to_owned(),
                        }),
                        proof,
                    };
                    self.bft_channels
                        .to_ctl_tx
                        .send(BftToCtlMsg::CommitBlock(pproof))
                        .unwrap();
                    return true;
                }
            }
        }
        // if height % TIMESTAMP_JUDGE_BLOCK_INTERVAL == 0 {
        //     let ms_now = AsMillis::as_millis(&unix_now());
        //     if ms_now > proposal_time
        //         && ms_now <= proposal_time + self.params.timer.get_total_duration()
        //     {
        //         self.start_time = ::std::time::Duration::from_millis(proposal_time);
        //     }
        // }
        false
    }

    fn pub_newview_message(&mut self, height: u64, round: u64) {
        self.pub_follower_message(height, round, Step::NewView, None);
    }

    fn pub_follower_message(&mut self, height: u64, round: u64, step: Step, hash: Option<H256>) {
        trace!(
            "Pub follower message {:?} {:?} {:?} {:?}",
            height,
            round,
            step,
            hash
        );
        let net_msg_type = {
            match step {
                Step::Prevote => VoteMsgType::LeaderPrevote,
                Step::Precommit => VoteMsgType::LeaderPrecommit,
                Step::NewView => VoteMsgType::NewView,
                _ => VoteMsgType::Noop,
            }
        };

        let vote = FollowerVote {
            height,
            round,
            step,
            hash,
        };

        let sig = self.sign_msg(vote.clone());
        let sv = SignedFollowerVote { vote, sig };
        self.send_raw_net_msg(net_msg_type.into(), 0, sv);
    }

    fn collect_votes(vote_set: &VoteSet, hash: H256) -> Vec<SignedFollowerVote> {
        let mut votes = Vec::new();
        for (sender, sign_vote) in &vote_set.votes_by_sender {
            if let Some(phash) = sign_vote.vote.hash {
                if phash != hash {
                    continue;
                }
                votes.push(sign_vote.to_owned());
            }
        }
        votes
    }

    fn pub_leader_message(&mut self, height: u64, round: u64, step: Step, hash: Option<H256>) {
        let net_msg_type = {
            match step {
                Step::Prevote => VoteMsgType::LeaderPrevote,
                Step::Precommit => VoteMsgType::LeaderPrecommit,
                _ => VoteMsgType::Noop,
            }
        };

        if hash.is_some() && !hash.unwrap().is_zero() {
            let send_votes = self
                .votes
                .get_voteset(height, round, step)
                .map(|vote_set| Self::collect_votes(&vote_set, hash.unwrap()));

            if let Some(votes) = send_votes {
                if self.is_above_threshold(votes.len() as u64) {
                    let lv = LeaderVote {
                        height,
                        round,
                        hash,
                        votes,
                    };
                    let lv_data: Vec<u8> = lv.into();
                    self.wal_save_message(height, LogType::QuorumVotes, &lv_data);
                    self.send_raw_net_msg(net_msg_type.into(), 0, lv_data)
                } else {
                    error!("Can't be here, Why vote is Not enough {:?}", votes.len());
                }
            }
        } else {
            self.send_raw_net_msg(net_msg_type.into(), 0, LeaderVote::default());
        }
    }

    fn check_proposal_proof(&self, pproof: ProposalWithProof) -> bool {
        let phash = pproof.proposal.unwrap().data.crypt_hash();

        let lvote: LeaderVote = deserialize(&pproof.proof).unwrap_or(LeaderVote::default());
        if !self.is_above_threshold(lvote.votes.len() as u64)
            || !self.is_above_threshold_old(lvote.votes.len() as u64)
        {
            return false;
        }
        if Some(phash) != lvote.hash {
            return false;
        }
        let h = lvote.height;
        let r = lvote.round;
        let mut senders = std::collections::HashSet::new();
        for sign_vote in &lvote.votes {
            let sender = Self::design_message(sign_vote.sig.clone(), sign_vote.vote.clone());
            if !self.is_validator(&sender) || !self.is_validator_old(&sender) {
                return false;
            }
            if sign_vote.vote.height != h
                || sign_vote.vote.round != r
                || sign_vote.vote.hash != lvote.hash
            {
                return false;
            }

            if !senders.insert(sender) {
                return false;
            }
        }
        true
    }

    fn add_leader_self_vote(
        &mut self,
        height: u64,
        round: u64,
        step: Step,
        hash: Option<H256>,
    ) -> bool {
        let vote = FollowerVote {
            height,
            round,
            step,
            hash,
        };
        let sig = self.sign_msg(vote.clone());
        self.votes.add(
            self.params.signer.address,
            &SignedFollowerVote { vote, sig },
        )
    }

    fn is_validator(&self, address: &Address) -> bool {
        self.auth_manage.validators.contains(address)
    }

    fn is_validator_old(&self, address: &Address) -> bool {
        self.auth_manage.validators_old.contains(address)
    }

    fn change_state_step(&mut self, height: u64, round: u64, step: Step) {
        trace!(
            "change_state_step {} -> {{ h: {}, r: {}, s: {} }}",
            self,
            height,
            round,
            step
        );
        self.height = height;
        self.round = round;
        self.step = step;
    }

    fn wal_save_state_step(&self, height: u64, round: u64, step: Step) -> Option<u64> {
        let message = serialize(&(height, round, self.nil_round, step)).unwrap();
        self.wal_save_message(height, LogType::State, &message)
    }

    fn wal_save_message(&self, height: u64, ltype: LogType, msg: &[u8]) -> Option<u64> {
        self.wal_log.borrow_mut().save(height, ltype, msg).ok()
    }

    fn wal_new_height(&self, height: u64) -> Option<u64> {
        self.wal_log.borrow_mut().set_height(height).ok()
    }

    fn handle_state(&mut self, msg: &[u8]) {
        if let Ok(decoded) = deserialize(msg) {
            let (h, r, nr, s) = decoded;
            self.height = h;
            self.round = r;
            self.nil_round = nr;
            self.step = s;
        }
    }

    fn check_leader_message(
        &mut self,
        step: Step,
        lvote: &LeaderVote,
    ) -> Result<(u64, u64, H256), EngineError> {
        let h = lvote.height;
        let r = lvote.round;

        if lvote.hash.is_none() || lvote.hash.unwrap().is_zero() {
            return Ok((h, r, H256::zero()));
        }

        if h < self.height || (h == self.height && r < self.round) {
            return Err(EngineError::VoteMsgDelay(r as usize));
        }

        if !self.is_above_threshold(lvote.votes.len() as u64) {
            return Err(EngineError::NotAboveThreshold(lvote.votes.len()));
        }

        for sign_vote in &lvote.votes {
            let sender = Self::design_message(sign_vote.sig.clone(), sign_vote.vote.clone());
            if !self.is_validator(&sender) {
                return Err(EngineError::NotAuthorized(sender));
            }
            if sign_vote.vote.height != h
                || sign_vote.vote.round != r
                || sign_vote.vote.hash != lvote.hash
            {
                return Err(EngineError::InvalidSignature);
            }

            /*bellow commit content is suit for when chain not syncing ,but consensus need
            process up */
            debug!(
                "Decode message get vote: \
                    height {}, \
                    round {}, \
                    step {}, \
                    sender {:?}, \
                    hash {:?}",
                h, r, step, sender, lvote.hash
            );

            let ret = self.votes.add(sender, &sign_vote);
            if !ret {
                debug!("Vote messsage add failed");
                return Err(EngineError::DoubleVote(sender));
            }
        }
        return Ok((h, r, lvote.hash.unwrap()));
    }

    fn follower_handle_message(
        &mut self,
        step: Step,
        net_msg: &NetworkMsg,
    ) -> Result<(u64, u64, H256), EngineError> {
        let lvote: LeaderVote =
            deserialize(&net_msg.msg).map_err(|_| EngineError::UnexpectedMessage)?;
        let ret = self.check_leader_message(step, &lvote);
        if let Ok((h, r, hash)) = ret {
            if h > self.height {
                return Err(EngineError::VoteMsgForth(h as usize));
            }
            self.wal_save_message(h, LogType::QuorumVotes, &net_msg.msg);
            return Ok((h, r, hash));
        }
        Err(ret.err().unwrap())
    }

    #[allow(clippy::type_complexity)]
    fn decode_basic_message(
        &self,
        content: &[u8],
    ) -> Result<(u64, u64, Step, Address, Option<H256>, Signature), EngineError> {
        if let Ok(decoded) = deserialize(&content[..]) {
            let (message, signature): (Vec<u8>, &[u8]) = decoded;
            if signature.len() != SIGNATURE_BYTES_LEN {
                return Err(EngineError::InvalidSignature);
            }
            let signature = Signature::from(signature);
            if let Ok(pubkey) = signature.recover(&message.crypt_hash()) {
                let decoded = deserialize(&message[..]).unwrap();
                let (h, r, step, sender, hash) = decoded;
                if pubkey_to_address(&pubkey) == sender && self.is_validator(&sender) {
                    return Ok((h, r, step, sender, hash, signature));
                }
            }
        }
        Err(EngineError::UnexpectedMessage)
    }

    fn handle_newview(&mut self, net_msg: &NetworkMsg) -> Result<(u64, u64), EngineError> {
        let fvote: SignedFollowerVote =
            deserialize(&net_msg.msg).map_err(|_| EngineError::UnexpectedMessage)?;

        let h = fvote.vote.height;
        let r = fvote.vote.round;

        if h < self.height {
            return Err(EngineError::VoteMsgDelay(h as usize));
        }
        let sender = Self::design_message(fvote.sig.clone(), fvote.vote.clone());
        if !self.is_validator(&sender) {
            return Err(EngineError::NotAuthorized(sender));
        }

        //deal with equal height,and round fall behind
        if h == self.height && r < self.round {
            let mut trans_flag = true;
            let now = Instant::now();

            let res = self.send_filter.get(&r);
            if let Some(instant_time) = res {
                if now - *instant_time
                    < self.params.timer.get_prevote()
                        * TIMEOUT_LOW_ROUND_FEED_MULTIPLE
                        * (self.auth_manage.validator_n() as u32 + 1)
                {
                    trans_flag = false;
                }
            }
            if trans_flag {
                self.send_filter.insert(r, now);
                self.pub_newview_message(h, r);
                trace!("Pub newview for old round h {} r {}", h, r);
            }
            return Err(EngineError::VoteMsgDelay(r as usize));
        }

        /*bellow commit content is suit for when chain not syncing ,but consensus need
        process up */
        if (h > self.height && h < self.height + self.auth_manage.validator_n() as u64 + 1)
            || (h == self.height && r >= self.round)
        {
            debug!(
                "Handle message hanle newview: \
                    height {}, \
                    round {}, \
                    sender {:?},",
                h, r, sender
            );
            let ret = self.votes.add(sender, &fvote);
            if ret {
                if h > self.height {
                    return Err(EngineError::VoteMsgForth(h as usize));
                }
                return Ok((h, r));
            }
            return Err(EngineError::DoubleVote(sender));
        }
        Err(EngineError::UnexpectedMessage)
    }

    fn leader_handle_message(
        &mut self,
        net_msg: &NetworkMsg,
    ) -> Result<(u64, u64, Option<H256>), EngineError> {
        let fvote: SignedFollowerVote =
            deserialize(&net_msg.msg).map_err(|_| EngineError::UnexpectedMessage)?;

        let h = fvote.vote.height;
        let r = fvote.vote.round;
        let s = fvote.vote.step;

        if h < self.height {
            return Err(EngineError::VoteMsgDelay(h as usize));
        }
        let sender = Self::design_message(fvote.sig.clone(), fvote.vote.clone());
        if !self.is_validator(&sender) {
            return Err(EngineError::NotAuthorized(sender));
        }

        if self
            .is_round_leader(h, r, &self.params.signer.address)
            .unwrap_or(false)
        {
            /*bellow commit content is suit for when chain not syncing ,but consensus need
            process up */
            if (h > self.height && h < self.height + self.auth_manage.validator_n() as u64 + 1)
                || (h == self.height && r >= self.round)
            {
                debug!(
                    "Handle message leader get vote: \
                     height {}, \
                     round {}, \
                     step {}, \
                     sender {:?}, \
                     hash {:?}",
                    h, r, s, sender, fvote.vote.hash,
                );
                let ret = self.votes.add(sender, &fvote);
                if ret {
                    if h > self.height {
                        return Err(EngineError::VoteMsgForth(h as usize));
                    }
                    return Ok((h, r, fvote.vote.hash));
                }
                return Err(EngineError::DoubleVote(sender));
            }
        }
        Err(EngineError::UnexpectedMessage)
    }

    // fn handle_message(
    //     &mut self,
    //     message: &[u8],
    // ) -> Result<(VoteType, u64, u64, Step, Option<H256>), EngineError> {
    //     if let Ok(decode) = deserialize(&message[..]) {
    //         let (vtype, content): (VoteType, Vec<u8>) = decode;
    //         if vtype == VoteType::FollowerTOLeaderVote {
    //             return self.leader_handle_message(&content[..]);
    //         } else if vtype == VoteType::LeaderAggregateVote {
    //             return self.follower_handle_message(&content[..]);
    //         } else {
    //             return self.uniform_handle_newview(&content[..]);
    //         }
    //     }
    //     Err(EngineError::UnexpectedMessage)
    // }

    fn follower_proc_proposal(&mut self, height: u64, round: u64) -> bool {
        let proposal = self.proposals.get_proposal(height, round);
        if let Some(proposal) = proposal {
            trace!("proc proposal {} begin h: {}, r: {}", self, height, round);

            if proposal.is_default() {
                info!("proc proposal is default empty");
                return false;
            }

            if !proposal.check(height, &self.auth_manage.validators) {
                warn!("Proc proposal check authorities error");
                return false;
            }

            let proposal_lock_round = proposal.lock_round;
            //we have lock block,try unlock
            if self.lock_round.is_some()
                && proposal_lock_round.is_some()
                && self.lock_round.unwrap() < proposal_lock_round.unwrap()
                && proposal_lock_round.unwrap() < round
            {
                //we see new lock block unlock mine
                info!(
                    "Proc proposal unlock locked block: height: {}, proposal: {:?}",
                    height, self.proposal
                );
                let msg = serialize(&self.lock_round.unwrap()).unwrap();
                let _ = self.wal_save_message(height, LogType::UnlockBlock, &msg);
                self.clean_proposal_locked_info();
            }
            self.proposal = Some(proposal.phash);
            return true;
        }
        debug!("Proc proposal not find proposal h {} r {}", height, round);
        false
    }

    fn set_proposal_and_block(&mut self, hash: Option<H256>, blk: Option<Vec<u8>>) {
        // self.proposal = hash;
        // self.locked_block = blk;
    }

    fn send_proposal_verify_req(&mut self, height: u64, round: u64, raw_proposal: Vec<u8>) {
        let _ = self
            .bft_channels
            .to_ctl_tx
            .send(BftToCtlMsg::CheckProposalReq(height, round, raw_proposal));
    }

    fn design_message<T: Into<Vec<u8>>>(signature: Signature, msg: T) -> Address {
        let msg: Vec<u8> = msg.into();
        let hash = msg.crypt_hash();
        if let Ok(pubkey) = signature.recover(&hash) {
            return pubkey_to_address(&pubkey);
        }
        Address::zero()
    }

    fn check_saved_proposal(
        &mut self,
        height: u64,
        round: u64,
        hash: &H256,
        raw_proposal: &[u8],
    ) -> Option<bool> {
        if let Some(pro_status) = self.hash_proposals.get_mut(hash) {
            match pro_status.1 {
                VerifiedProposalStatus::Init => {
                    self.send_proposal_verify_req(height, round, raw_proposal.to_owned())
                }
                VerifiedProposalStatus::Ok => {
                    return Some(true);
                }

                _ => {
                    return Some(false);
                }
            }
        } else {
            self.hash_proposals.insert(
                hash.to_owned(),
                (raw_proposal.to_owned(), VerifiedProposalStatus::Init),
            );
        }
        None
    }

    fn handle_proposal(
        &mut self,
        net_msg: &NetworkMsg,
        wal_flag: bool,
    ) -> Result<(u64, u64), EngineError> {
        trace!("Handle proposal {} begin wal_flag: {}", self, wal_flag);
        let sign_proposal: SignedNetworkProposal =
            deserialize(&net_msg.msg).map_err(|_| EngineError::UnexpectedMessage)?;

        let height = sign_proposal.proposal.height;
        let round = sign_proposal.proposal.round;
        let signature = sign_proposal.sig;
        if signature.len() != SIGNATURE_BYTES_LEN {
            return Err(EngineError::InvalidSignature);
        }

        if height < self.height
            || (height == self.height && round < self.round)
            || (height == self.height && round == self.round && self.step > Step::ProposeWait)
        {
            debug!("Handle proposal {} get old proposal", self);
            return Err(EngineError::VoteMsgDelay(height as usize));
        } else if height == self.height && self.step == Step::CommitWait {
            debug!(
                "Not handle proposal {} because conensus is ok in height",
                self
            );
            return Err(EngineError::VoteMsgForth(round as usize));
        }

        trace!(
            "handle proposal {} hash {:?}",
            self,
            sign_proposal.proposal.vote_proposal.phash
        );

        if height < self.height
            || (height == self.height && round < self.round)
            || (height == self.height && round == self.round && self.step > Step::ProposeWait)
        {
            debug!("Handle proposal {} get old proposal", self);
            return Err(EngineError::VoteMsgDelay(height as usize));
        } else if height == self.height && self.step == Step::CommitWait {
            debug!(
                "Not handle proposal {} because conensus is ok in height",
                self
            );
            return Err(EngineError::VoteMsgForth(round as usize));
        }

        let sender = Self::design_message(signature, sign_proposal.proposal.clone());
        let ret = self.is_round_leader(height, round, &sender);
        if !ret.unwrap_or(false) {
            warn!("Handle proposal {},{:?} is not round leader ", self, sender);
            return Err(EngineError::NotProposer(Mismatch {
                expected: Address::default(),
                found: sender,
            }));
        }

        trace!(
            "Handle proposal {} h: {}, r: {}, sender: {:?}",
            self,
            height,
            round,
            sender
        );

        let check_res = self.check_saved_proposal(
            height,
            round,
            &sign_proposal.proposal.vote_proposal.phash,
            &sign_proposal.proposal.raw_proposal,
        );

        //self.leader_origins.insert((height, round), origin);
        if height >= self.height && height < self.height + self.auth_manage.validator_n() as u64 + 1
        {
            if wal_flag {
                self.wal_save_message(height, LogType::Propose, &net_msg.msg)
                    .unwrap();
            }
            debug!(
                "Handle proposal {} add proposal h: {}, r: {}",
                self, height, round
            );

            self.proposals
                .add(height, round, sign_proposal.proposal.vote_proposal);

            if height == self.height {
                match check_res {
                    Some(res) => {
                        if !res {
                            self.proposal = None;
                        }
                        return Ok((height, round));
                    }
                    None => {
                        return Err(EngineError::InvalidTxInProposal);
                    }
                }
            }
            return Err(EngineError::VoteMsgForth(height as usize));
        }

        Err(EngineError::UnexpectedMessage)
    }

    /// Clean proof and chain hash result
    fn clean_proof_hash(&mut self) {
        // if self.height > INIT_HEIGHT {
        //     self.proof = self.proof.split_off(&(self.height - 1));
        //     self.height_hashes = self.height_hashes.split_off(&(self.height - 1));
        // }
    }

    /// Clean origin of height,round
    fn clean_leader_origins(&mut self) {
        if self.height > INIT_HEIGHT {
            self.leader_origins = self.leader_origins.split_off(&(self.height, 0));
        }
    }

    /// Clean proposal and locked info
    fn clean_proposal_locked_info(&mut self) {
        self.proposal = None;
        self.lock_round = None;
        self.last_commit_round = None;
    }

    fn clean_verified_info(&mut self, height: u64) {
        //self.unverified_msg = self.unverified_msg.split_off(&(height + 1, 0));
    }

    fn clean_block_txs(&mut self) {
        self.block_txs = self.block_txs.split_off(&self.height);
    }

    fn clean_height_timestamps(&mut self) {
        self.height_timestamps = self.height_timestamps.split_off(&(self.height - 1));
    }

    fn clean_filter_info(&mut self) {
        self.send_filter.clear();
    }

    fn clean_proposal_when_verify_failed(&mut self) {
        let height = self.height;
        let round = self.round;
        self.clean_proposal_locked_info();
        self.pub_follower_message(height, round, Step::Prevote, Some(H256::default()));
        self.pub_follower_message(height, round, Step::Precommit, Some(H256::default()));
        self.change_state_step(height, round, Step::NewView);
        self.pub_newview_message(height, round);
    }

    fn sign_msg<T>(&self, can_hash: T) -> Signature
    where
        T: Into<Vec<u8>>,
    {
        let message: Vec<u8> = can_hash.into();
        let author = &self.params.signer;
        let hash = message.crypt_hash();
        Signature::sign(author.keypair.privkey(), &hash).unwrap()
    }

    pub fn leader_new_proposal(&mut self, save_flag: bool) -> bool {
        let mut sign_prop = SignedNetworkProposal::new();

        if let Some(lock_round) = self.lock_round {
            if let Some(proposal) = self.proposals.get_proposal(self.height, lock_round) {
                if self.search_history_proposal(proposal.phash) {
                    self.proposal = Some(proposal.phash);
                    self.proposals
                        .add(self.height, self.round, proposal.clone());
                    let cp = NetworkProposal::new_with_proposal(self.height, self.round, proposal);
                    let sig = self.sign_msg(cp.clone());
                    sign_prop.set(cp, sig);
                    info!("New proposal proposal lock block {:?}", self);
                } else {
                    warn!("not find proposal raw data");
                    return false;
                }
            } else {
                warn!("not find locked proposal?");
                return false;
            }
        } else {
            if self.self_proposal.is_none() {
                return false;
            }
            self.proposal = self.self_proposal;
            let p = Proposal::new(self.self_proposal.unwrap());
            self.proposals.add(self.height, self.round, p.clone());
            let cp = NetworkProposal::new_with_proposal(self.height, self.round, p);
            let sig = self.sign_msg(cp.clone());
            sign_prop.set(cp, sig);
            info!("New proposal proposal {:?}", self);
        }

        self.pub_proposal(&sign_prop);
        if save_flag {
            let msg: Vec<u8> = sign_prop.into();
            self.wal_save_message(self.height, LogType::Propose, &msg);
            self.add_leader_self_vote(self.height, self.round, Step::Prevote, self.proposal);
        }
        true
    }

    fn check_vote_count(&mut self, height: u64, round: u64, step: Step) -> bool {
        let vote_set = self.votes.get_voteset(height, round, step);
        if let Some(vote_set) = vote_set {
            if self.is_above_threshold(vote_set.count) {
                return true;
            }
        }
        false
    }

    fn follower_prevote_send(&mut self, height: u64, round: u64) {
        let prop = self.proposal;
        if self.lock_round.is_some() || prop.is_some() {
            self.pub_follower_message(height, round, Step::Prevote, prop);
        } else {
            trace!("Follower prevote {} have nothing", self);
            self.pub_follower_message(height, round, Step::Prevote, Some(H256::default()));
        }
    }

    fn newview_timeout_vote(&mut self, height: u64, round: u64) {
        trace!(
            "Node_process_timeout vote self {} h {} r {}",
            self,
            height,
            round
        );
        self.pub_newview_message(height, round);
        self.change_state_step(height, round, Step::NewView);
        self.set_state_timeout(
            height,
            round,
            Step::NewView,
            self.params.timer.get_prevote()
                * TIMEOUT_RETRANSE_MULTIPLE
                * (self.auth_manage.validator_n() as u32 + 1),
        );
    }

    fn leader_proc_timeout_vote(&mut self, height: u64, round: u64, step: Step) {
        trace!("Leader_proc_timeout_vote h {} r {}", height, round);
        if self.check_vote_count(height, round, step) {
            self.pub_leader_message(height, round, step, None);
            self.new_round_start(height, round + 1);
        } else {
            self.newview_timeout_vote(height, round);
            if self.is_only_one_node() {
                self.new_round_start(height, round + 1);
            }
        }
    }

    pub fn timeout_process(&mut self, tminfo: &TimeoutInfo) {
        trace!(
            "Timeout process {} tminfo: {}, wait {:?}",
            self,
            tminfo,
            unix_now().checked_sub(self.start_time)
        );

        if tminfo.height < self.height
            || (tminfo.height == self.height && tminfo.round < self.round)
            || (tminfo.height == self.height
                && tminfo.round == self.round
                && tminfo.step != self.step)
        {
            debug!(
                " Timeout process tminfo {:?} self {} not equal",
                tminfo, self
            );
            return;
        }

        match tminfo.step {
            Step::Propose => {}
            Step::ProposeAuth => {}
            Step::ProposeWait | Step::PrevoteWait | Step::PrecommitWait => {
                let step = {
                    if tminfo.step == Step::PrecommitWait {
                        Step::Precommit
                    } else {
                        Step::Prevote
                    }
                };

                let ret =
                    self.is_round_leader(tminfo.height, tminfo.round, &self.params.signer.address);
                if ret.is_ok() && ret.unwrap() {
                    self.leader_proc_timeout_vote(tminfo.height, tminfo.round, step);
                } else {
                    self.newview_timeout_vote(tminfo.height, tminfo.round);
                }
            }

            Step::Prevote => {
                // follower not be here
                self.leader_proc_timeout_vote(tminfo.height, tminfo.round, Step::Prevote);
            }

            Step::Precommit => {
                /*in this case,need resend prevote : my net server can be connected but other node's
                server not connected when staring.  maybe my node receive enough vote(prevote),but others
                did not receive enough vote,so even if my node step precommit phase, i need resend prevote also.
                */
                // self.pre_proc_prevote(tminfo.height, tminfo.round);
                // self.pre_proc_precommit();
                self.leader_proc_timeout_vote(tminfo.height, tminfo.round, Step::Precommit);
            }
            Step::Commit => {
                self.do_commit_work(tminfo.height, tminfo.round);
            }
            Step::CommitPending => {
                self.send_proposal_request();
            }
            Step::CommitWait => {
                if self.deal_old_height_when_commited(tminfo.height) {
                    self.new_round_start(tminfo.height + 1, INIT_ROUND);
                }
            }
            Step::NewView => {
                self.newview_timeout_vote(tminfo.height, tminfo.round);
                self.proc_new_view(tminfo.height, tminfo.round);
            }
        }
    }

    // For clippy
    #[allow(clippy::cognitive_complexity)]
    pub fn process_network(&mut self, net_msg: NetworkMsg) {
        match net_msg.r#type.as_str().into() {
            VoteMsgType::Proposal => {
                if let Ok((height, round)) = self.handle_proposal(&net_msg, true) {
                    self.follower_proc_proposal(height, round);
                    self.bundle_op_after_proposal(height, round);
                }
            }
            VoteMsgType::Prevote => {
                if let Ok((h, r, hash)) = self.leader_handle_message(&net_msg) {
                    self.leader_proc_prevote(h, r, hash);
                }
            }
            VoteMsgType::Precommit => {
                if let Ok((h, r, hash)) = self.leader_handle_message(&net_msg) {
                    self.leader_proc_precommit(h, r, hash);
                }
            }
            VoteMsgType::LeaderPrevote => {
                if let Ok((h, r, hash)) = self.follower_handle_message(Step::Prevote, &net_msg) {
                    self.follower_proc_prevote(h, r, hash);
                }
            }
            VoteMsgType::LeaderPrecommit => {
                if let Ok((h, r, hash)) = self.follower_handle_message(Step::Prevote, &net_msg) {
                    self.follower_proc_precommit(h, r, Some(hash));
                }
            }
            _ => {}
        }
    }

    fn set_state_timeout(&self, height: u64, round: u64, step: Step, delay: Duration) {
        trace!(
            "Set state timeout: {:?},{:?},{:?},{:?},",
            height,
            round,
            step,
            delay
        );
        let now = Instant::now();
        let _ = self.bft_channels.to_timer_tx.send(TimeoutInfo {
            timeval: now + delay,
            height,
            round,
            step,
        });
    }

    fn proposal_interval_round_multiple(&self, round: u64) -> Duration {
        let real_round = (round + 1).wrapping_sub(self.nil_round.0);
        self.params.timer.get_propose() * (real_round as u32)
    }

    /// New round proposal wait interval should be exponentially times increase
    fn proposal_interval_round_exp(&self, round: u64) -> Duration {
        let coef = {
            let real_round = round.wrapping_sub(self.nil_round.0);
            if real_round > MAX_PROPOSAL_TIME_COEF {
                MAX_PROPOSAL_TIME_COEF
            } else {
                real_round
            }
        };
        self.params.timer.get_propose() * 2u32.pow(coef as u32)
    }

    fn new_round_start(&mut self, height: u64, round: u64) {
        self.new_round_start_with_added_time(height, round, Duration::new(0, 0));
    }

    fn new_round_start_with_added_time(&mut self, height: u64, round: u64, added_time: Duration) {
        self.change_state_step(height, round, Step::Propose);
        if round == INIT_ROUND {
            self.start_time = unix_now() + added_time;
            // When this node in lower height, maybe other node's
            // Newview message already arrived
            if !self.is_only_one_node() && self.proc_new_view(height, round) {
                return;
            }
        }

        let mut tv = self.proposal_interval_round_exp(round);
        let mut step = Step::ProposeWait;
        if self
            .is_round_leader(height, round, &self.params.signer.address)
            .unwrap_or(false)
        {
            if self.leader_new_proposal(true) {
                step = Step::PrevoteWait;
                // The code is for only one Node
                if self.is_only_one_node() {
                    info!("new round in only one node h {} r {}", height, round);
                    self.leader_proc_prevote(height, round, None);
                    if self.leader_proc_precommit(height, round, None) {
                        return;
                    }
                }
            }
            tv += added_time;
        } else if self.follower_proc_proposal(height, round) {
            self.follower_prevote_send(height, round);
            step = Step::PrevoteWait;
            tv = self.proposal_interval_round_multiple(round);
        }

        self.change_state_step(height, round, step);
        self.wal_save_state_step(height, round, step);
        self.set_state_timeout(height, round, step, tv);
    }

    pub fn redo_work(&mut self) {
        let height = self.height;
        let round = self.round;
        let step = self.step;
        trace!("Redo work {} begin", self);
        match step {
            Step::Propose | Step::ProposeAuth | Step::ProposeWait => {
                self.new_round_start(height, round);
            }
            Step::Prevote | Step::PrevoteWait => {
                if let Ok(is_leader) =
                    self.is_round_leader(height, round, &self.params.signer.address)
                {
                    if is_leader {
                        self.add_leader_self_vote(height, round, Step::Prevote, self.proposal);
                        self.leader_proc_prevote(height, round, None);
                    } else {
                        self.follower_prevote_send(height, round);
                        self.follower_proc_prevote(
                            height,
                            round,
                            self.proposal.unwrap_or(H256::default()),
                        );
                    }
                }

                if self.step == Step::PrevoteWait {
                    self.set_state_timeout(
                        height,
                        round,
                        Step::PrevoteWait,
                        self.proposal_interval_round_multiple(round),
                    );
                }
            }
            Step::Precommit | Step::PrecommitWait => {
                if let Ok(is_leader) =
                    self.is_round_leader(height, round, &self.params.signer.address)
                {
                    if is_leader {
                        if self.lock_round.is_some() {
                            self.add_leader_self_vote(
                                height,
                                round,
                                Step::Precommit,
                                self.proposal,
                            );
                        }
                        self.leader_proc_precommit(height, round, None);
                    } else {
                        self.pre_proc_precommit();
                        if let Some(hash) = self.check_saved_vote(height, round, Step::Precommit) {
                            self.follower_proc_precommit(height, round, Some(hash));
                        }
                    }
                }
                if self.step == Step::PrecommitWait {
                    self.set_state_timeout(
                        height,
                        round,
                        Step::PrecommitWait,
                        self.proposal_interval_round_multiple(round),
                    );
                }
            }
            Step::Commit | Step::CommitPending | Step::CommitWait => {
                /*when rebooting ,we did not know chain if is ready
                    if chain garantee that when I sent commit_block,
                    it can always issue block, no need for this.
                */
                self.do_commit_work(height, round);
            }
            Step::NewView => {
                self.newview_timeout_vote(height, round);
            }
        }
    }

    fn load_wal_log(&mut self) {
        let vec_buf = self.wal_log.borrow_mut().load();
        self.height = self.wal_log.borrow().get_cur_height();
        self.round = INIT_ROUND;
        for (mtype, vec_out) in vec_buf {
            let log_type: LogType = mtype.into();
            trace!("load_wal_log {} type {:?}({})", self, log_type, mtype);
            match log_type {
                LogType::Skip => {}
                LogType::Propose => {
                    // let res = self.uniform_handle_proposal(&vec_out[..], false);
                    // if let Ok((_h, _r)) = res {
                    //     // let proposal = self.proposals.get_proposal(h, r).unwrap();
                    //     // let compact_block = CompactBlock::try_from(&proposal.block).unwrap();
                    //     // self.set_proposal_and_block(
                    //     //     Some(compact_block.crypt_hash()),
                    //     //     Some(compact_block),
                    //     // );
                    // }
                }
                LogType::Vote => {
                    // let res = self.decode_basic_message(&vec_out[..]);
                    // if let Ok((h, r, s, sender, hash, signature)) = res {
                    //     self.votes.add(
                    //         h,
                    //         r,
                    //         s,
                    //         sender,
                    //         &VoteMessage {
                    //             proposal: hash,
                    //             signature,
                    //         },
                    //     );
                    // }
                }
                LogType::State => {
                    self.handle_state(&vec_out[..]);
                }
                LogType::PrevHash => {}
                LogType::Commits => {
                    if let Ok(proof) = deserialize(&vec_out) {
                        let proof: BftProof = proof;
                        trace!("load_wal_log {} wal proof height: {:?}", self, proof.height);
                        //Wself.proof.insert(proof.height as u64, proof);
                    }
                }
                LogType::VerifiedPropose => {
                    // if let Ok(decode) = deserialize(&vec_out) {
                    //     let (vheight, vround, verified): (u64, u64, i8) = decode;
                    //     let status: VerifiedProposalStatus = verified.into();
                    //     trace!(
                    //         "load_wal_log {} LogType::VerifiedPropose status {:?}",
                    //         self,
                    //         status
                    //     );
                    //     if status.is_ok() {
                    //         self.unverified_msg.remove(&(vheight, vround));
                    //     } else {
                    //         self.clean_proposal_locked_info();
                    //     }
                    // }
                }
                LogType::VerifiedBlock => {
                    //     if let Ok(decode) = deserialize(&vec_out) {
                    //         let (vheight, vround, verified, bytes): (u64, u64, i8, Vec<u8>) =
                    //             decode;
                    //         let status: VerifiedProposalStatus = verified.into();
                    //         let block = Block::try_from(&bytes).unwrap();
                    //         if status.is_ok() {
                    //             let bhash = block.crypt_hash();
                    //             trace!("LogType::VerifiedBlock set block hash {:?}", bhash);
                    //             self.verified_blocks.push((bhash, block));
                    //             self.unverified_msg.remove(&(vheight, vround));
                    //         } else {
                    //             self.clean_proposal_locked_info();
                    //         }
                    //     }
                }
                LogType::AuthTxs => {
                    // trace!("load_wal_log {} LogType::AuthTxs begin", self);
                    // let blocktxs = BlockTxs::try_from(&vec_out);
                    // if let Ok(blocktxs) = blocktxs {
                    //     let height = blocktxs.get_height() as u64;
                    //     trace!(
                    //         "load_wal_log {} LogType::AuthTxs add height: {}",
                    //         self,
                    //         height
                    //     );
                    //     self.block_txs.insert(height+1, blocktxs);
                    // }
                }

                LogType::QuorumVotes => {
                    if let Ok(decoded) = deserialize(&vec_out) {
                        let (height, round, step, hash, locked_vote): (
                            u64,
                            u64,
                            Step,
                            H256,
                            VoteSet,
                        ) = decoded;

                        let find_flag = {
                            // if !self.search_history_proposal(
                            //     height,
                            //     self.lock_round.unwrap_or(round),
                            //     hash,
                            // ) {
                            //     self.search_history_verified_block(hash)
                            // } else {
                            //     true
                            // }
                            true
                        };

                        if find_flag {
                            info!("QuorumVotes set proposal lock {:?}", self.proposal);
                            if step == Step::Prevote {
                                self.lock_round = Some(round);
                                //self.locked_vote = Some(locked_vote.clone());
                            } else {
                                self.last_commit_round = Some(round);
                            }

                            for (sender, vote_msg) in locked_vote.votes_by_sender {
                                self.votes.add(sender, &vote_msg);
                            }
                        } else {
                            self.set_proposal_and_block(None, None);
                        }
                    }
                }

                LogType::UnlockBlock => {
                    if let Ok(decoded) = deserialize(&vec_out) {
                        let _round: u64 = decoded;
                        self.clean_proposal_locked_info();
                    }
                }
            }
        }
        trace!("load_wal_log ends");
    }

    fn recv_new_height_signal(&mut self, h: u64, proposal: Vec<u8>) {
        let hash = proposal.crypt_hash();
        self.hash_proposals
            .insert(hash, (proposal, VerifiedProposalStatus::Ok));

        if h <= self.height {
            return;
        }
        self.self_proposal = Some(hash);
        let mut added_time = Duration::new(0, 0);

        let now = unix_now();
        if let Some(gone_time) = now.checked_sub(self.start_time) {
            let config_interval = Duration::from_millis(self.params.timer.get_total_duration());
            if h == self.height + 1 && gone_time < config_interval {
                added_time = config_interval - gone_time;
            }
        }

        if !self
            .is_round_leader(h, INIT_ROUND, &self.params.signer.address)
            .unwrap_or(false)
            || self.is_only_one_node()
        {
            self.change_state_step(self.height, self.round, Step::CommitWait);
            self.set_state_timeout(self.height, self.round, Step::CommitWait, added_time);
            return;
        }

        if self.deal_old_height_when_commited(self.height) {
            self.new_round_start_with_added_time(h, INIT_ROUND, added_time);
        }
    }

    fn bundle_op_after_proposal(&mut self, height: u64, round: u64) {
        self.follower_prevote_send(height, round);
        self.change_state_step(height, round, Step::PrevoteWait);
        self.set_state_timeout(
            height,
            round,
            Step::PrevoteWait,
            self.proposal_interval_round_multiple(round),
        );
    }

    pub async fn start(&mut self) {
        self.load_wal_log();
        // TODO : broadcast some message, based on current state
        if self.height >= INIT_HEIGHT {
            //self.send_proof_request();
            self.redo_work();
        }

        loop {
            tokio::select! {
                svrmsg = self.bft_channels.to_bft_rx.recv() => {
                    info!("recv to bft msg {:?}",svrmsg);
                    if let Some(svrmsg) = svrmsg {
                        match svrmsg {
                            BftSvrMsg::Conf(config) => {
                                self.params.timer.set_total_duration(config.block_interval as u64);
                                let mut validators = Vec::new();
                                for v in config.validators {
                                    validators.push(Address::from(&v[..]));
                                }
                                self.auth_manage.receive_authorities_list(
                                                config.height as usize,
                                                &validators,
                                                &validators,
                                            );

                                if self.is_validator(&self.params.signer.address) {
                                    self.is_consensus_node = true;
                                } else {
                                    self.is_consensus_node = false;
                                }
                            },
                            BftSvrMsg::PProof(pproof,tx) => {
                                let res = self.check_proposal_proof(pproof);
                                tx.send(res).unwrap();
                            }
                        }
                    }
                },

                cback = self.bft_channels.ctl_back_rx.recv() => {
                    info!("recv to control back msg {:?}",cback);
                    if let Some(cback) = cback {
                        match cback {
                            CtlBackBftMsg::GetProposalRes(height,proposal) => {
                                self.recv_new_height_signal(height,proposal);
                            },
                            CtlBackBftMsg::CheckProposalRes(height,round,res) => {
                                let hash = self.proposals.get_proposal(height,round).map(|p| p.phash);
                                if let Some(hash) = hash {
                                    if height == self.height && round == self.round && self.step < Step::Prevote {
                                        trace!(
                                            "Process {} recieve check proposal  res h: {}, r: {} res {}",
                                            self,
                                            height,
                                            round,
                                            res,
                                        );
                                        if !res {
                                            self.proposal = None;
                                            self.hash_proposals.remove(&hash);
                                        } else {
                                            self.hash_proposals.get_mut(&hash).map(|res| res.1=VerifiedProposalStatus::Ok);
                                            self.follower_proc_proposal(height, round);
                                        }
                                        self.bundle_op_after_proposal(height,round);
                                    }
                                }
                            }
                            CtlBackBftMsg::CommitBlockRes => {
                                self.send_proposal_request();
                            }
                        }
                    }
                },
                net_msg = self.bft_channels.net_back_rx.recv() => {
                    info!("recv to network back msg {:?}",net_msg);
                    if let Some(net_msg) = net_msg {
                        self.process_network(net_msg);
                    }

                },
                tminfo = self.bft_channels.timer_back_rx.recv() => {
                    info!("recv to timer msg {:?}",tminfo);
                    if let Some(tminfo) = tminfo {
                        self.timeout_process(&tminfo);
                    }
                }
            }
        }
    }

    fn send_proposal_request(&mut self) {
        self.bft_channels
            .to_ctl_tx
            .send(BftToCtlMsg::GetProposalReq)
            .unwrap();

        trace!("send_proposal_request height {} ", self.height);

        self.change_state_step(self.height, self.round, Step::CommitPending);
        self.set_state_timeout(
            self.height,
            self.round,
            Step::CommitPending,
            self.params.timer.get_prevote(),
        );
    }
}
