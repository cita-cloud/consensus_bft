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
use std::convert::{From, Into};

use crate::params::BftParams;
use crate::voteset::{Proposal, ProposalCollector, VoteCollector, VoteSet};

use crate::votetime::TimeoutInfo;
use crate::wal::{LogType, Wal};
use cita_logger::{debug, error, info, trace, warn};

use crate::crypto::{pubkey_to_address, CreateKey, Sign, Signature, SIGNATURE_BYTES_LEN};
use crate::message::{
    BftSvrMsg, BftToCtlMsg, CompactProposal, CompactSignedProposal, CtlBackBftMsg, FollowerVote,
    SignedFollowerVote, Step, Vote,LeaderVote,VoteMsgType
};
use crate::types::{Address, H256};
use cita_cloud_proto::common::ProposalWithProof;
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

#[cfg(feature = "timestamp_test")]
#[derive(Debug, Clone, Copy)]
pub enum SignSymbol {
    Positive,
    Zero,
    Negative,
}

#[cfg(feature = "timestamp_test")]
#[derive(Debug)]
pub struct TimeOffset {
    sym: SignSymbol,
    off: u64,
}

#[cfg(feature = "timestamp_test")]
impl TimeOffset {
    pub fn new(sym: SignSymbol, off: u64) -> TimeOffset {
        TimeOffset { sym, off }
    }
}

pub enum BftTurn {
    Message(TransType),
    Timeout(TimeoutInfo),
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub enum VoteType {
    FollowerTOLeaderVote,
    LeaderAggregateVote,
    BoadcastVote,
}

#[derive(Debug, Clone, Copy)]
enum VerifiedBlockStatus {
    Ok,
    Err,
    Init(u8),
}

impl VerifiedBlockStatus {
    pub fn value(self) -> i8 {
        match self {
            VerifiedBlockStatus::Ok => 1,
            VerifiedBlockStatus::Err => -1,
            VerifiedBlockStatus::Init(_) => 0,
        }
    }

    pub fn is_ok(self) -> bool {
        match self {
            VerifiedBlockStatus::Ok => true,
            _ => false,
        }
    }

    pub fn is_init(self) -> bool {
        match self {
            VerifiedBlockStatus::Init(_) => true,
            _ => false,
        }
    }
}

impl From<i8> for VerifiedBlockStatus {
    fn from(s: i8) -> Self {
        match s {
            1 => VerifiedBlockStatus::Ok,
            -1 => VerifiedBlockStatus::Err,
            0 => VerifiedBlockStatus::Init(0),
            _ => panic!("Invalid VerifiedBlockStatus."),
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
    // pub_sender: Sender<PubType>,
    // timer_seter: Sender<TimeoutInfo>,
    // receiver: Receiver<BftTurn>,
    params: BftParams,
    height: u64,
    round: u64,

    // NilRound.0: nil round 1. last nil propose round
    nil_round: NilRound,
    step: Step,
    // proof: BTreeMap<u64, BftProof>,
    hash_proposals: BTreeMap<H256, Vec<u8>>,
    // Timestamps of old blocks
    height_timestamps: BTreeMap<u64, u64>,
    votes: VoteCollector,
    proposals: ProposalCollector,
    proposal: Option<H256>,
    locked_proposal: Option<H256>,
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
    unverified_msg: BTreeMap<(u64, u64), (Vec<u8>, VerifiedBlockStatus)>,
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
        Bft {
            // pub_sender: s,
            // timer_seter: ts,
            // receiver: r,
            params,
            height: 0,
            round: INIT_ROUND,
            nil_round: (INIT_ROUND, INIT_ROUND),
            step: Step::Propose,
            hash_proposals: BTreeMap::new(),
            height_timestamps: BTreeMap::new(),
            votes: VoteCollector::new(),
            proposals: ProposalCollector::new(),
            proposal: None,
            lock_round: None,
            locked_proposal: None,
            wal_log: RefCell::new(Wal::create(&*logpath).unwrap()),
            send_filter: BTreeMap::new(),
            last_commit_round: None,
            start_time: unix_now(),
            auth_manage: AuthorityManage::new(),
            is_consensus_node: false,
            unverified_msg: BTreeMap::new(),
            leader_origins: BTreeMap::new(),
            block_txs: BTreeMap::new(),
            block_proof: None,
            verified_blocks: Vec::new(),
            is_cleared: false,
            version: None,
            bft_channels,
            #[cfg(feature = "timestamp_test")]
            mock_time_modify: TimeOffset::new(SignSymbol::Zero, 0),
        }
    }

    async fn send_raw_net_msg<T: Into<Vec<u8>>>(&self, rtype: &str, origin: u64, msg: T) {
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
        if p.authorities.is_empty() {
            warn!("There are no authorities");
            return Err(EngineError::NotAuthorized(Address::zero()));
        }
        let proposer_nonce = height + round;
        let proposer: &Address = p
            .authorities
            .get(proposer_nonce as usize % p.authorities.len())
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

    pub fn pub_proposal(&mut self, compact_signed_proposal: &CompactSignedProposal) {
        self.send_raw_net_msg(
            VoteMsgType::Proposal.into(),
            0,
            compact_signed_proposal.clone(),
        );
    }

    fn search_history_verified_block(&mut self, hash: H256) -> bool {
        if let Some(block) = self.get_verified_block(&hash) {
            // self.set_proposal_and_block(Some(hash), Some(block.compact()));
            return true;
        }
        false
    }

    fn follower_proc_prevote(&mut self, height: u64, round: u64, hash: H256) -> bool {
        trace!("Follower proc prevote hash {:?} self {:?}", hash, self);
       
        let old_lock_round = self.unlock_polc(round);

        let vote_set = self.votes.get_voteset(height, round, Step::Prevote);
        if let Some(vset) = vote_set {
            if !hash.is_zero() {
                // Try effort to find real proposal block
                if self.proposal != Some(hash) {
                    warn!(
                        "Follower proc prevote proposal {:?} not equal hash {:?} lock_round {:?}",
                        self.proposal,hash,old_lock_round
                    );
                    if !self.search_history_proposal(
                        height,
                        old_lock_round.unwrap_or(round),
                        hash,
                    ) {
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

                        if let Some(hash) =
                            self.check_saved_vote(height, round, Step::Precommit)
                        {
                            self.follower_proc_precommit(height, round, Some(hash));
                        }
                    }
                    return true;
                }
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

    fn uniform_proc_new_view(&mut self, height: u64, round: u64) -> bool {
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
            if !self.params.issue_nil_block {
                // Find the honest nil round
                let fnum = self.get_faulty_limit_number();
                let mut sum = 0;
                for (hash, count) in vote_set.unwrap().votes_by_proposal.iter().rev() {
                    sum += count;
                    if sum > fnum {
                        let nround = hash.low_u64() as u64;
                        if self.nil_round.0 < nround {
                            self.nil_round.0 = nround;
                        }
                    }
                }
            }
            self.pub_newview_message(height, round);
            self.new_round_start(height, round + 1);
            return true;
        } else if self.is_only_one_node() {
            self.new_round_start(height, round + 1);
        }
        false
    }

    fn search_history_proposal(&mut self, _height: u64, _round: u64, checked_hash: H256) -> bool {

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

    fn leader_proc_prevote(&mut self, net_msg :&NetworkMsg) -> bool {
        let sign_vote = deserialize(&net_msg.msg);
        if sign_vote.is_err() {
            return false;
        }
        let sign_vote : SignedFollowerVote = sign_vote.unwrap();
        let height = sign_vote.vote.height;
        let round = sign_vote.vote.round;

        debug!(
            "Leader proc prevote {} begin h: {}, r: {}",
            self, height, round
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
                let mut addrs = Vec::new();

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
                                if !self.search_history_proposal(
                                    height,
                                    old_lock_round.unwrap_or(round),
                                    *hash,
                                ) {
                                    self.proposal = Some(*hash);
                                }
                            }
                            if self.proposal == Some(*hash) {
                                debug!("Leader proc prevote proposal {:?}", self.proposal);
                                // Here means has locked in earlier round
                                self.lock_round = Some(round);
                                //self.locked_vote = Some(vote_set.clone());
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
                            let vmsg =
                                serialize(&(height, round, Step::Precommit, *hash, &vote_set))
                                    .unwrap();
                            let _ = self.wal_save_message(height, LogType::QuorumVotes, &vmsg);
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

            //no need to this step
            // if self.step == Step::Precommit || (self.round < round && self.step < Step::Commit)
            // {
            //     self.change_state_step(height, round, Step::PrecommitWait, false);
            //     let now = Instant::now();
            //     let _ = self.timer_seter.send(TimeoutInfo {
            //         timeval: now + tv,
            //         height,
            //         round,
            //         step: Step::PrecommitWait,
            //     });
            // }
        }
        false
    }

    fn get_faulty_limit_number(&self) -> u64 {
        self.auth_manage.validator_n() as u64 / 3
    }

    fn is_above_threshold(&self, n: u64) -> bool {
        n * 3 > self.auth_manage.validator_n() as u64 * 2
    }

    fn is_equal_threshold(&self, n: u64) -> bool {
        n == (self.auth_manage.validator_n() as u64 * 2) / 3
    }

    fn is_all_vote(&self, n: u64) -> bool {
        n == self.auth_manage.validator_n() as u64
    }

    fn get_proposal_verified_result(&self, height: u64, round: u64) -> VerifiedBlockStatus {
        self.unverified_msg
            .get(&(height, round))
            .map_or(VerifiedBlockStatus::Ok, |res| res.1)
    }

    fn pre_proc_precommit(&mut self) -> bool {
        let height = self.height;
        let round = self.round;
        let mut lock_ok = false;

        trace!("pre proc precommit begin {}", self);

        if let Some(lround) = self.lock_round {
            if lround == round {
                lock_ok = true;
            }
        }
        let verify_result = self.get_proposal_verified_result(height, round);

        trace!(
            "pre proc precommit {} locked round {:?} verify result {:?}",
            self,
            self.lock_round,
            verify_result
        );

        //polc is ok,but not verified , not send precommit
        if lock_ok && verify_result.is_init() {
            return false;
        }

        if lock_ok && verify_result.is_ok() {
            self.pub_follower_message(height, round, Step::Precommit, self.proposal);
        } else {
            self.pub_follower_message(height, round, Step::Precommit, Some(H256::default()));
        }

        // let now = Instant::now();
        // //timeout for resending vote msg
        // let _ = self.timer_seter.send(TimeoutInfo {
        //     timeval: now + (self.params.timer.get_precommit() * TIMEOUT_RETRANSE_MULTIPLE),
        //     height: self.height,
        //     round: self.round,
        //     step: Step::Precommit,
        // });
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
        if self.uniform_check_and_commit_work(height, round) {
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
                if self.proposal == Some(hash) {
                    self.last_commit_round = Some(round);

                    if let Some(vote_set) = self.votes.get_voteset(height, round, Step::Precommit) {
                        let vmsg =
                            serialize(&(height, round, Step::Precommit, hash, vote_set)).unwrap();
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

    fn uniform_check_and_commit_work(&mut self, height: u64, round: u64) -> bool {
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
                    return self.commit_block();
                }
            }
        }
        trace!("uniform check and commit_work failed");
        false
    }

    fn wal_save_proof(&self, _height: u64, _proof: &BftProof) {
        //let bmsg = serialize(proof, Infinite).unwrap();
        //let _ = self.wal_save_message(height + 1, LogType::Commits, &bmsg);
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

    fn generate_proof(&mut self, height: u64, round: u64, hash: H256) -> Option<BftProof> {
        let mut commits = HashMap::new();
        {
            let vote_set = self.votes.get_voteset(height, round, Step::Precommit);
            let mut num: u64 = 0;
            if let Some(vote_set) = vote_set {
                for (sender, sign_vote) in &vote_set.votes_by_sender {
                    if sign_vote.vote.hash.is_none() {
                        continue;
                    }
                    if sign_vote.vote.hash.unwrap() == hash {
                        num += 1;
                        commits.insert(*sender, sign_vote.signature.clone());
                    }
                }
            }
            if !self.is_above_threshold(num) {
                return None;
            }
        }
        let mut proof = BftProof::default();
        proof.height = height as usize;
        proof.round = round as usize;
        proof.proposal = hash;
        proof.commits = commits;
        Some(proof)
    }

    fn commit_block(&mut self) -> bool {
        // Commit the block using a complete signature set.
        let height = self.height;

        //to be optimize
        self.clean_verified_info(height);
        trace!("commit_block {:?} begin", self);
        // if let Some(hash) = self.proposal {
        // if !self.proof.contains_key(&height) {
        //     if let Some(lround) = self.last_commit_round {
        //         if let Some(p) = self.generate_proof(height, lround, hash) {
        //             self.wal_save_proof(height, &p);
        //             self.proof.insert(height, p);
        //         }
        //     }
        // }

        //     if let Some(proof) = self.proof.get(&height).cloned() {
        //         if self.locked_block.is_some() {
        //             let locked_block = self.locked_block.clone().unwrap();
        //             let locked_block_hash = locked_block.crypt_hash();

        //             // The self.locked_block is a compact block.
        //             // So, fetch the bodies of transactions from self.verified_blocks.
        //             if let Some(proposal_block) = self.get_verified_block(&locked_block_hash) {
        //                 // let proposal_time = proposal_block.get_header().get_timestamp();
        //                 // let mut proof_blk = ProposalWithProof::new();

        //                 // // proof_blk.set_blk(proposal_block);
        //                 // // proof_blk.set_proof(proof.into());

        //                 // // saved for retranse blockwithproof to chain
        //                 // self.block_proof = Some((height, proof_blk.clone()));
        //                 // info!(
        //                 //     "commit block {} consensus time {:?} proposal {} locked block hash {}",
        //                 //     self,
        //                 //     unix_now().checked_sub(self.start_time),
        //                 //     hash,
        //                 //     locked_block_hash,
        //                 // );
        //                 // self.pub_block(&proof_blk);

        //                 // if height % TIMESTAMP_JUDGE_BLOCK_INTERVAL == 0 {
        //                 //     let ms_now = AsMillis::as_millis(&unix_now());
        //                 //     if ms_now > proposal_time
        //                 //         && ms_now <= proposal_time + self.params.timer.get_total_duration()
        //                 //     {
        //                 //         self.start_time = ::std::time::Duration::from_millis(proposal_time);
        //                 //     }
        //                 // }

        //                 return true;
        //             } else {
        //                 info!("commit block {} verified blocks is not ok", self);
        //             }
        //         } else {
        //             info!("commit block {} locked_block is not ok", self);
        //         }
        //     } else {
        //         info!("commit block {} proof is not ok", self);
        //     }
        // }
        //goto next round
        false
    }

    fn pub_message(&self, vtype: VoteType, content: Vec<u8>) {
        self.pub_message_raw(vtype, content, None);
    }

    fn pub_message_raw(&self, vtype: VoteType, content: Vec<u8>, origin: Option<u32>) {
        // let message = serialize(&(vtype, content)).unwrap();
        // let mut msg: Message = message.into();
        // if let Some(origin) = origin {
        //     //msg.set_operate(OperateType::Single);
        //     msg.set_origin(origin);
        // }
        // let omsg = msg.try_into().unwrap();
        // self.pub_sender
        //     .send((routing_key!(Consensus >> RawBytes).into(), omsg))
        //     .unwrap();
    }

    fn pub_newview_message(&mut self, height: u64, round: u64) {
        let author = &self.params.signer;
        let hash = {
            if self.params.issue_nil_block {
                None
            } else {
                Some(H256::from(self.nil_round.0 as u64))
            }
        };
        let msg = serialize(&(height, round, Step::NewView, author.address, hash)).unwrap();
        let signature = Signature::sign(author.keypair.privkey(), &msg.crypt_hash()).unwrap();
        let msg = serialize(&(msg, signature)).unwrap();
        self.pub_message(VoteType::BoadcastVote, msg);
    }

    fn pub_follower_message(&mut self, height: u64, round: u64, step: Step, hash: Option<H256>) {
        trace!(
            "Pub follower message {:?} {:?} {:?} {:?}",
            height,
            round,
            step,
            hash
        );
        let vote = FollowerVote {
            height,
            round,
            step,
            hash,
        };

        let sig = self.sign_msg(vote.clone());

        let sv = SignedFollowerVote { vote, sig };

        self.send_raw_net_msg(VoteMsgType::Prevote.into(), 0, sv);
    }

    fn collect_votes(vote_set: &VoteSet,hash:H256) ->Vec<SignedFollowerVote> {
        let mut votes = Vec::new();
        for (sender, sign_vote) in vote_set.votes_by_sender {
            if let Some(phash) = sign_vote.vote.hash {
                if phash != hash {
                    continue;
                }
                votes.push(sign_vote);
            }
        }
    }

    fn pub_leader_message(&mut self, height: u64, round: u64, step: Step, hash: Option<H256>) {
        let net_msg_type = {
            match step {
                step::prevote => VoteMsgType::LeaderPrevote,
                step::precommit => VoteMsgType::LeaderPrecommit,
                _ => VoteMsgType::Noop,
            }
        };

        if  hash.is_some() && !hash.unwrap().is_zero() {
                let hash = hash.unwrap();
                let send_votes = self.votes.get_voteset(height, round, step)
                .map(|vote_set| {
                    Self::collect_votes(vote_set,hash)
                 });

                 if let Some(votes) = send_votes {
                    if self.is_above_threshold(send_votes.len() as u64) {
                        let msg = serialize(&(height, round, step, Some(hash), send_votes)).unwrap();
                        trace!(
                            "Leader pub success aggregate msg hash {:?} len {} step {:?}",
                            hash,
                            msg.len(),
                            step
                        );
                        self.pub_message(VoteType::LeaderAggregateVote, msg);

                        let lv = LeaderVote {height,round,hash,votes};
                        let lv_data:Vec<u8> = lv.into();
                        self.wal_save_message(height, LogType::QuorumVotes, lv_data.clone());
                        self.send_raw_net_msg(net_msg_type.into(), 0, lv_data)
                    } else {
                        error!(
                            "Can't be here, Why vote is Not enough {:?}",
                            send_votes.len()
                        );
                    }
                 }
        } else {
            self.send_raw_net_msg(net_msg_type.into(), 0, LeaderVote::default());
        }
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
        let sig = self.sign_msg(vote);
        self.votes.add(
            self.params.signer.address,
            &SignedFollowerVote {
                vote,
                sig,
            },
        )
    }

    fn is_validator(&self, address: &Address) -> bool {
        self.auth_manage.validators.contains(address)
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
        step:Step,
        lvote: &LeaderVote,
    ) -> Result<(u64,u64,H256), EngineError> {
        let h = lvote.height;
        let r = lvote.round;

        if lvote.hash.is_none() || lvote.hash.unwrap().is_zero() {
            return Ok((h,r, H256::zero()));
        }

        if h < self.height || (h == self.height && r < self.round) {
            return Err(EngineError::VoteMsgDelay(r as usize));
        }

        if !self.is_above_threshold(lvote.votes.len() as u64) {
            return Err(EngineError::NotAboveThreshold(lvote.votes.len()));
        }

        for sign_vote in lvote.votes {
            let sender = Self::design_message(vote.sig, sign_vote.vote);
            if !self.is_validator(&sender) {
                return Err(EngineError::NotAuthorized(sender));
            }
            if sign_vote.vote.height!=h || sign_vote.vote.round!=r || sign_vote.vote.hash != lvote.hash {
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
                    hash {:?}", \
                h, r, s, sender, lvote.hash);
            
            let ret = self.votes.add(
                sender,
                &sign_vote
            );
                if !ret {
                    debug!("Vote messsage add failed");
                    return Err(EngineError::DoubleVote(sender));
                }
        }
        return Ok((h, r, lvote.hash.unwrap()));
    }

    fn handle_newview(
        &mut self,
        content: &[u8],
    ) -> Result<(VoteType, u64, u64, Step, Option<H256>), EngineError> {
        let decoded = self.decode_basic_message(content)?;
        let (h, r, step, sender, hash, signature) = decoded;

        trace!(
            "Decode newview message {} parse success h: {}, r: {}, s: {},hash {:?}, sender: {:?}",
            self,
            h,
            r,
            step,
            hash,
            sender,
        );

        if h < self.height {
            return Err(EngineError::VoteMsgDelay(h as usize));
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
                "Handle new view get vote: \
                 height {}, \
                 round {}, \
                 step {}, \
                 sender {:?}, \
                 hash {:?} ",
                h, r, step, sender, hash
            );
            // let ret = self.votes.add(
                
            //     sender,
            //     &VoteMessage {
            //         proposal: hash,
            //         signature,
            //     },
            // );
            if true {
                if h > self.height {
                    return Err(EngineError::VoteMsgForth(h as usize));
                }

                return Ok((VoteType::BoadcastVote, h, r, step, hash));
            }
            return Err(EngineError::DoubleVote(sender));
        }
        Err(EngineError::UnexpectedMessage)
    }

    fn follower_handle_message(
        &mut self,
        step:Step,
        net_msg: &NetworkMsg,
    ) -> Result<(u64,u64,H256), EngineError> {
        let lvote : LeaderVote = deserialize(net_msg.msg).map_err(EngineError::ErrFormatted)?;
        let ret = self.check_leader_message(step,lvote);
        if let Ok((h, r,  hash)) = ret {
            if h > self.height {
                return Err(EngineError::VoteMsgForth(h as usize));
            }

            self.wal_save_message(h, LogType::QuorumVotes, &lvote.into());
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

    fn leader_handle_message(
        &mut self,
        fvote: &SignedFollowerVote,
    ) -> Result<(), EngineError> {
       let h = fvote.vote.height;
       let r = fvote.vote.round;
       let s = fvote.vote.step;

        if h < self.height {
            return Err(EngineError::VoteMsgDelay(h as usize));
        }
        let sender = Self::design_message(fvote.sig, fvote.vote);
        if !self.is_validator(sender) {
            return Err(EngineError::BadSignature());
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
                     hash {:?}," 
                    h, r, s, sender, fvote.vote.hash, 
                );
                let ret = self.votes.add(sender,fvote);
                if ret {
                    if h > self.height {
                        return Err(EngineError::VoteMsgForth(h as usize));
                    }
                    return Ok(());
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
    //     Err(EngineError::ErrFormatted)
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

            //     let block = CompactBlock::try_from(&proposal.block).unwrap();
            //     if !self.verify_version(&block) {
            //         warn!(
            //             "Proc proposal {} version error h: {}, r: {}",
            //             self, height, round
            //         );
            //         return false;
            //     }

            //     let btime = block.get_header().get_timestamp();
            //     if !self.verify_timestamp(height, btime) {
            //         warn!(
            //             "Proc proposal {} timestamp error h: {}, r: {}",
            //             self, height, round
            //         );
            //         return false;
            //     }

            //     //proof : self.params vs proposal's block's broof
            //     let block_proof = block.get_header().get_proof();
            //     let proof = BftProof::from(block_proof.clone());
            //     debug!(
            //         "Proc proposal h: {}, r: {}, proof: {:?}",
            //         height, round, proof
            //     );
            //     if self.auth_manage.authority_h_old == height - 1 {
            //         if !proof.check(height - 1, &self.auth_manage.validators_old) {
            //             warn!(
            //                 "Proof check error h {} validator old {:?}",
            //                 height, self.auth_manage.validators_old,
            //             );
            //             return false;
            //         }
            //     } else if !proof.check(height - 1, &self.auth_manage.validators) {
            //         warn!(
            //             "Proof check error h {} validator {:?}",
            //             height, self.auth_manage.validators,
            //         );
            //         return false;
            //     }

            //     self.proof.entry(height - 1).or_insert(proof);

            //     //prehash : self.prehash vs  proposal's block's prehash

            //     let mut block_prehash = Vec::new();
            //     block_prehash.extend_from_slice(block.get_header().get_prevhash());

            //     //height 1's block not have prehash
            //     if let Some(hash) = self.height_hashes.get(&(height - 1)) {
            //         if *hash != H256::from(block_prehash.as_slice()) {
            //             warn!(
            //                 "Proc proposal {} pre_hash h: {}, r: {} error hash {:?} saved {:?}",
            //                 self,
            //                 height,
            //                 round,
            //                 *hash,
            //                 H256::from(block_prehash.as_slice())
            //             );
            //             return false;
            //         }
            //     } else if height != INIT_HEIGHT {
            //         return false;
            //     }

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
            // still lock on a blk,next prevote it
            // if self.lock_round.is_some() {
            //     let locked_block = self.locked_block.clone().unwrap();
            //     self.set_proposal_and_block(Some(locked_block.crypt_hash()), Some(locked_block));
            //     info!("Proc proposal still have locked block {:?}", self);
            // } else {
            //     // else use proposal block, self.lock_round is none
            //     let compact_block = CompactBlock::try_from(&proposal.block).unwrap();
            //     self.set_proposal_and_block(Some(compact_block.crypt_hash()), Some(compact_block));
            //     debug!("Proc proposal save the proposal's hash {:?}", self);
            // }
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

    // fn verify_version(&self, block: &CompactBlock) -> bool {
    //     if self.version.is_none() {
    //         warn!("Verify version {} self.version is none", self);
    //         return false;
    //     }
    //     let version = self.version.unwrap();
    //     if block.get_version() != version {
    //         warn!(
    //             "Verify version {} failed block version: {}, current chain version: {}",
    //             self,
    //             block.get_version(),
    //             version
    //         );
    //         false
    //     } else {
    //         true
    //     }
    // }

    fn verify_proposal_req(&mut self, height: u64, round: u64, raw_proposal: Vec<u8>) {
        let _ = self
            .bft_channels
            .to_ctl_tx
            .send(BftToCtlMsg::CheckProposalReq(height, round, raw_proposal));
    }

    fn verify_timestamp(&self, height: u64, pro_ts: u64) -> bool {
        if height == 0 {
            return true;
        }
        if let Some(pre_ts) = self.height_timestamps.get(&(height - 1)) {
            let cur_ts: u64 = AsMillis::as_millis(&unix_now());
            if pro_ts <= *pre_ts
                || (pro_ts > cur_ts + TIMESTAMP_DIFF_MAX_INTERVAL
                    && pro_ts > *pre_ts + self.params.timer.get_total_duration())
            {
                warn!(
                    "Proc_proposal {} timestamp error h: {}, current timestamp {}, propoal timestamp {}, pre block timestamp {}, upper limit {}",
                    self, height, cur_ts, pro_ts, pre_ts, self.params.timer.get_total_duration()
                );
                return false;
            }

            if pro_ts > cur_ts {
                warn!(
                    "Proc_proposal {} timestamp warning h: {},, current timestamp {}, propoal timestamp {}",
                    self, height, cur_ts, pro_ts,
                );
            }
            return true;
        }
        false
    }

    fn design_message<T: Into<Vec<u8>>>(signature:Signature,msg: &T) -> Address {
        let msg :Vec<u8> = msg.into();
        let hash = msg.crypt_hash();
        if let Ok(pubkey) = signature.recover(&hash) {
            return pubkey_to_address(&pubkey);
        }
        Address::zero()
    }

    fn handle_proposal(
        &mut self,
        net_msg: &NetworkMsg,
        wal_flag: bool,
    ) -> Result<(u64, u64), EngineError> {
        trace!("Handle proposal {} begin wal_flag: {}", self, wal_flag);
        let sign_proposal: CompactSignedProposal =
            deserialize(&net_msg.msg).map_err(|_| EngineError::ErrFormatted)?;

        let height = sign_proposal.proposal.height;
        let round = sign_proposal.proposal.round;
        let signature =sign_proposal.sig;
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

        trace!("handle proposal {} hash {:?}", self, sign_proposal.proposal.vote_proposal.phash);

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

        let sender = Self::design_message(signature, &sign_proposal.proposal);

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

            // let compact_block = compact_proposal.clone().take_block();
            // if !self.params.issue_nil_block
            //     && compact_block.get_body().get_tx_hashes().is_empty()
            // {
            //     info!("Handle proposal {} get nil body", self);

            //     if round > self.nil_round.1 {
            //         self.nil_round.1 = round;
            //         self.nil_round.0 += 1;
            //     }

            //     self.send_txlist_request(self.height - 1);
            //     // TODO other error type
            //     return Err(EngineError::NilProposal);
            // }

            self.verify_proposal_req(
                sign_proposal.proposal.height,
                sign_proposal.proposal.round,
                sign_proposal.proposal.raw_proposal.clone(),
            );
            self.hash_proposals
                .insert(sign_proposal.proposal.vote_proposal, sign_proposal.proposal.raw_proposal);
           
            //self.leader_origins.insert((height, round), origin);
            if (height == self.height && round >= self.round)
                || (height > self.height
                    && height < self.height + self.auth_manage.validator_n() as u64 + 1)
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

                if height > self.height {
                    return Err(EngineError::VoteMsgForth(height as usize));
                }
                return Ok((height, round));
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
        self.locked_proposal = None;
        self.last_commit_round = None;
    }

    fn clean_verified_info(&mut self, height: u64) {
        self.unverified_msg = self.unverified_msg.split_off(&(height + 1, 0));
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
        let mut sign_prop = CompactSignedProposal::new();
        let mut cp: CompactProposal;

        if let Some(lock_round) = self.lock_round {
            if let Some(proposal) = self.proposals.get_proposal(self.height, lock_round) {
                self.proposal = Some(proposal.phash);
                self.proposals
                    .add(self.height, self.round, proposal.clone());

                cp = CompactProposal::new_with_proposal(self.height, self.round, proposal);
            } else {
                warn!("not find locked proposal?");
                return false;
            }

            let sig = self.sign_msg(cp.clone());
            sign_prop.set(cp, sig);
            info!("New proposal proposal lock block {:?}", self);
        } else {
            let p = Proposal::new(self.proposal.unwrap());
            let cp = CompactProposal::new_with_proposal(self.height, self.round, p);
            let sig = self.sign_msg(cp.clone());
            sign_prop.set(cp, sig);
            info!("New proposal proposal {:?}", self);
        }

        self.pub_proposal(&sign_prop);
        if save_flag {
            let msg: Vec<u8> = sign_prop.into();
            self.wal_save_message(self.height, LogType::Propose, &msg);
            //     .unwrap();
            // if self.lock_round.is_none() {
            //     let msg = serialize(&(
            //         self.height,
            //         self.round,
            //         VerifiedBlockStatus::Ok.value(),
            //         sign_prop.into(),
            //     ))
            //     .unwrap();
            //     self.wal_save_message(self.height, LogType::VerifiedBlock, &msg);
            // }

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

    fn uniform_newview_timeout_vote(&mut self, height: u64, round: u64) {
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
            self.uniform_newview_timeout_vote(height, round);
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
                    self.uniform_newview_timeout_vote(tminfo.height, tminfo.round);
                }
            }

            Step::Prevote => {
                // follower not be here
                self.leader_proc_timeout_vote(tminfo.height, tminfo.round, Step::Prevote);
            }

            Step::PrecommitAuth => {
                //     let mut wait_too_many_times = false;
                //     let mut verify_delay_flag = false;
                //     // If consensus doesn't receive the result of block verification in a specific
                //     // time-frame, use the original message to construct a request, then resend it to auth.
                //     if let Some((csp_msg, result)) =
                //         self.unverified_msg.get_mut(&(tminfo.height, tminfo.round))
                //     {
                //         if let VerifiedBlockStatus::Init(ref mut times) = *result {
                //             trace!("Wait for the verification result {} times", times);
                //             if *times >= 3 {
                //                 error!("Do not wait for the verification result again");
                //                 wait_too_many_times = true;
                //             } else {
                //                 let verify_req = csp_msg
                //                     .clone()
                //                     .take_compact_signed_proposal()
                //                     .unwrap()
                //                     .create_verify_block_req();
                //                 let mut msg: Message = verify_req.into();
                //                 msg.set_origin(csp_msg.get_origin());
                //                 // self.pub_sender
                //                 //     .send((
                //                 //         routing_key!(Consensus >> VerifyBlockReq).into(),
                //                 //         msg.try_into().unwrap(),
                //                 //     ))
                //                 //     .unwrap();
                //                 verify_delay_flag = true;
                //                 *times += 1;
                //             };
                //             if verify_delay_flag {
                //                 self.set_state_timeout(
                //                     tminfo.height,
                //                     tminfo.round,
                //                     Step::PrecommitAuth,
                //                     self.params.timer.get_prevote()
                //                         * TIMEOUT_RETRANSE_MULTIPLE
                //                         * (self.auth_manage.validator_n() as u32 + 1),
                //                 );
                //             }
                //         } else {
                //             warn!("Already get verified result {:?}", *result);
                //         }
                //     };
                //     // If waited the result of verification for a long while, we consider it was failed.
                //     if wait_too_many_times {
                //         self.clean_proposal_when_verify_failed();
                //     }
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
            Step::CommitWait => {
                if self.deal_old_height_when_commited(tminfo.height) {
                    self.new_round_start(tminfo.height + 1, INIT_ROUND);
                }
            }
            Step::NewView => {
                self.uniform_newview_timeout_vote(tminfo.height, tminfo.round);
                self.uniform_proc_new_view(tminfo.height, tminfo.round);
            }
        }
    }

    fn follower_proc_prevote_raw(
        &mut self,
        height: u64,
        round: u64,
        hash: Option<H256>,
        vres: VerifiedBlockStatus,
    ) {
        hash.map_or_else(
            || {},
            |hash| {
                if hash.is_zero() {
                    self.follower_proc_prevote(height, round, Some(hash));
                } else {
                    match vres {
                        VerifiedBlockStatus::Ok => {
                            self.follower_proc_prevote(height, round, Some(hash));
                        }
                        VerifiedBlockStatus::Init(_) => {
                            self.change_state_step(height, round, Step::PrecommitAuth);
                            self.set_state_timeout(
                                height,
                                round,
                                Step::PrecommitAuth,
                                self.params.timer.get_precommit()
                                    * TIMEOUT_RETRANSE_MULTIPLE
                                    * (self.auth_manage.validator_n() as u32 + 1),
                            );
                        }
                        VerifiedBlockStatus::Err => self.clean_proposal_when_verify_failed(),
                    }
                }
            },
        );
    }

    fn wrap_follower_proc_prevote_hash(
        &mut self,
        height: u64,
        round: u64,
        vres: VerifiedBlockStatus,
    ) {
        let hash = self.check_saved_vote(height, round, Step::Prevote);
        self.follower_proc_prevote_raw(height, round, hash, vres);
    }

    fn wrap_follower_proc_prevote_verify(&mut self, height: u64, round: u64, hash: Option<H256>) {
        let vres = self.get_proposal_verified_result(height, round);
        self.follower_proc_prevote_raw(height, round, hash, vres);
    }

    fn wrap_follower_proc_prevote(&mut self, height: u64, round: u64) {
        let hash = self.check_saved_vote(height, round, Step::Prevote);
        let vres = self.get_proposal_verified_result(height, round);
        self.follower_proc_prevote_raw(height, round, hash, vres);
    }

    // For clippy
    #[allow(clippy::cognitive_complexity)]
    pub fn process_network(&mut self, net_msg: NetworkMsg) {
    
        match net_msg.r#type.as_str().into() {
            VoteMsgType::Proposal => {
                let _res = self.handle_proposal(&net_msg, true);
            }
            VoteMsgType::Prevote => {
                if self.leader_handle_message(&net_msg).is_ok() {
                    self.leader_proc_prevote(net_msg);
                }
            }
            VoteMsgType::Precommit => {}
            VoteMsgType::LeaderPrevote => {
                if let Ok((h,r,hash))= self.follower_handle_message(Step::Prevote,&net_msg) {
                    self.follower_proc_prevote(h,r,hash);
                }
                
            }
            VoteMsgType::LeaderPrecommit => {}
            _ => {}
        };

        if true {
            //if from_broadcast && self.is_consensus_node {
            // match rtkey {
            //     routing_key!(Net >> CompactSignedProposal) => {
            //         let res = self.uniform_handle_proposal(&body[..], true);
            //         if let Ok((height, round)) = res {
            //             trace!(
            //                 "Process {} recieve handle proposal ok; h: {}, r: {}",
            //                 self,
            //                 height,
            //                 round,
            //             );

            //             let pres = self.follower_proc_proposal(height, round);
            //             if pres {
            //                 self.follower_prevote_send(height, round);
            //                 self.change_state_step(height, round, Step::PrevoteWait);
            //                 self.set_state_timeout(
            //                     height,
            //                     round,
            //                     Step::PrevoteWait,
            //                     self.proposal_interval_round_multiple(round),
            //                 );
            //             } else {
            //                 info!("Process {} proc proposal failed;", self);
            //             }
            //         } else {
            //             info!(
            //                 "Process {} fail handle proposal {}",
            //                 self,
            //                 res.err().unwrap()
            //             );
            //         }
            //     }

            //     routing_key!(Net >> RawBytes) => {
            //         let raw_bytes = msg.take_raw_bytes().unwrap();
            //         let res = self.handle_message(&raw_bytes[..]);

            //         if let Ok((vtype, h, r, s, hash)) = res {
            //             if vtype == VoteType::FollowerTOLeaderVote {
            //                 if s == Step::Prevote {
            //                     self.leader_proc_prevote(h, r, hash);
            //                 } else if s == Step::Precommit {
            //                     self.leader_proc_precommit(h, r, hash);
            //                 } else {
            //                     self.uniform_proc_new_view(h, r);
            //                 }
            //             } else if s == Step::Prevote {
            //                 self.wrap_follower_proc_prevote_verify(h, r, hash);
            //             } else if s == Step::Precommit {
            //                 self.follower_proc_precommit(h, r, hash);
            //             } else {
            //                 self.uniform_proc_new_view(h, r);
            //             }
            //         }
            //     }
            //     _ => {}
            // }
        } else {
            // match rtkey {
            //     // accept authorities_list from chain
            //     routing_key!(Chain >> RichStatus) => {
            //         let rich_status = msg.take_rich_status().unwrap();
            //         trace!(
            //             "Process {} get new local status {:?}",
            //             self,
            //             rich_status.height
            //         );
            //         self.receive_new_status(&rich_status);
            //         let authorities: Vec<Address> = rich_status
            //             .get_nodes()
            //             .iter()
            //             .map(|node| Address::from_slice(node))
            //             .collect();
            //         trace!("Authorities: [{:?}]", authorities);

            //         let validators: Vec<Address> = rich_status
            //             .get_validators()
            //             .iter()
            //             .map(|node| Address::from_slice(node))
            //             .collect();
            //         trace!("Validators: [{:?}]", validators);

            //         if validators.contains(&self.params.signer.address) {
            //             self.is_consensus_node = true;
            //         } else {
            //             info!(
            //                 "Address[{:?}] is not consensus node !",
            //                 self.params.signer.address
            //             );
            //             self.is_consensus_node = false;
            //         }
            //         self.auth_manage.receive_authorities_list(
            //             rich_status.height as u64,
            //             &authorities,
            //             &validators,
            //         );
            //         let version = rich_status.get_version();
            //         trace!("verison: {}", version);
            //         if self.version.is_none() && !self.proof.contains_key(&self.height) {
            //             self.send_proof_request();
            //         }
            //         self.version = Some(version);
            //     }
            //     routing_key!(Chain >> InvalidLicense) => {
            //         error!(
            //             "CITA license Invalid: {}",
            //             String::from_utf8(body).expect("Cannot convert license error to string!")
            //         );
            //         exit(1);
            //     }
            //     routing_key!(Chain >> BlockWithProof) => {
            //         let mut bproof = msg.take_block_with_proof().unwrap();
            //         let proof = BftProof::from(bproof.take_proof());
            //         trace!("Chain proof comming {}", proof.height);
            //         self.proof.insert(proof.height, proof);
            //     }
            //     routing_key!(Auth >> VerifyBlockResp) => {
            //         let resp = msg.take_verify_block_resp().unwrap();

            //         let block = resp.get_block();
            //         let vheight = resp.get_height() as u64;
            //         let vround = resp.get_round() as u64;

            //         let verify_res = if resp.get_pass() {
            //             // Save the verified block which has passed verification by the auth.
            //             self.verified_blocks
            //                 .push((block.crypt_hash(), block.clone()));
            //             VerifiedBlockStatus::Ok
            //         } else {
            //             VerifiedBlockStatus::Err
            //         };

            //         if verify_res.is_ok() {
            //             let block_bytes: Vec<u8> = block.try_into().unwrap();
            //             let msg = serialize(
            //                 &(vheight, vround, verify_res.value(), block_bytes)
            //             )
            //             .unwrap();

            //             self.wal_save_message(vheight, LogType::VerifiedBlock, &msg);
            //         } else {
            //             let tmp = vec![0];
            //             let msg = serialize(&(vheight, vround, verify_res.value(), tmp))
            //                 .unwrap();
            //             self.wal_save_message(vheight, LogType::VerifiedBlock, &msg);
            //         }

            //         if let Some(res) = self.unverified_msg.get_mut(&(vheight, vround)) {
            //             res.1 = verify_res;
            //             // Send SignedProposal to executor.
            //             if let Some(compact_signed_proposal) =
            //                 res.0.clone().take_compact_signed_proposal()
            //             {
            //                 let signed_proposal = compact_signed_proposal
            //                     .complete(block.get_body().get_transactions().to_vec());
            //                 let msg: Message = signed_proposal.into();
            //                 // self.pub_sender
            //                 //     .send((
            //                 //         routing_key!(Consensus >> SignedProposal).into(),
            //                 //         msg.try_into().unwrap(),
            //                 //     ))
            //                 //     .unwrap();
            //             }
            //         };

            //         info!(
            //             "Process {} recieve Auth VerifyBlockResp h: {}, r: {}, resp: {:?}",
            //             self, vheight, vround, verify_res,
            //         );
            //         if vheight == self.height
            //             && vround == self.round
            //             && self.step == Step::PrecommitAuth
            //         {
            //             //verify not ok,so clean the proposal info
            //             self.wrap_follower_proc_prevote_hash(vheight, vround, verify_res);
            //         }
            //     }

            //     routing_key!(Auth >> BlockTxs) => {
            //         let block_txs = msg.take_block_txs().unwrap();
            //         debug!(
            //             "Process {} recieve BlockTxs h: {}",
            //             self,
            //             block_txs.get_height(),
            //         );
            //         let height = block_txs.get_height() as u64;
            //         //let msg: Vec<u8> = (&block_txs).try_into().unwrap();

            //         if !block_txs.get_body().get_transactions().is_empty() {
            //             self.block_txs.insert(height + 1, block_txs);
            //         } else {
            //             self.block_txs.entry(height + 1).or_insert(block_txs);
            //         }

            //         // if self.params.issue_nil_block {
            //         //     self.wal_save_message(height+1, LogType::AuthTxs, &msg);
            //         // }

            //         if self.height == height + 1
            //             && self
            //                 .is_round_leader(self.height, self.round, &self.params.signer.address)
            //                 .unwrap_or(false)
            //             && self.step == Step::ProposeWait
            //             && self.proposal.is_none()
            //         {
            //             self.leader_new_proposal();
            //             if self.is_only_one_node() {
            //                 self.leader_proc_prevote(self.height, self.round, None);
            //                 self.leader_proc_precommit(self.height, self.round, None);
            //             }
            //         }
            //     }

            //     _ => {}
            // }
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
        // let _ = self.timer_seter.send(TimeoutInfo {
        //     timeval: now + delay,
        //     height,
        //     round,
        //     step,
        // });
    }

    // fn receive_new_status(&mut self, status: &RichStatus) {
    //     let status_height = status.height as u64;

    //     if status.interval == 0 {
    //         self.params.timer.set_total_duration(DEFAULT_TIME_INTERVAL);
    //         self.params.issue_nil_block = false;
    //     } else {
    //         self.params.timer.set_total_duration(status.interval);
    //         self.params.issue_nil_block = true;
    //     }

    //     let height = self.height;
    //     let step = self.step;
    //     trace!(
    //         "Receive new status {} receive height {} interval {}",
    //         self,
    //         status_height,
    //         status.interval,
    //     );
    //     if height > 0 && status_height + 1 < height {
    //         return;
    //     }

    //     // try efforts to save previous hash,when current block is not commit to chain
    //     let pre_hash = H256::from_slice(&status.hash);
    //     self.height_hashes.entry(status_height).or_insert(pre_hash);
    //     self.height_timestamps
    //         .entry(status_height)
    //         .or_insert(status.timestamp);

    //     // For Chain,deal with repeated new status
    //     if height > 0 && status_height + 1 == height {
    //         // commit timeout since chain not has or not completing exec block,
    //         // so resending the block
    //         if step >= Step::Commit {
    //             if let Some((hi, ref bproof)) = self.block_proof {
    //                 if hi == height {
    //                     self.pub_block(bproof);
    //                 }
    //             }
    //         }
    //         return;
    //     }
    //     // higher or equal height status
    //     let new_round = if status_height == height {
    //         self.wal_new_height(height + 1);
    //         // Try my effor to save proof,
    //         // when I skipping commit_blcok by the chain sending new status.
    //         if !self.proof.contains_key(&status_height) {
    //             if let Some(hash) = self.proposal {
    //                 let res = self
    //                     .last_commit_round
    //                     .and_then(|lround| self.generate_proof(status_height, lround, hash));
    //                 if let Some(proof) = res {
    //                     self.wal_save_proof(status_height, &proof);
    //                     self.proof.insert(status_height, proof);
    //                 }
    //             }
    //         }
    //         self.round
    //     } else {
    //         INIT_ROUND
    //     };

    //     let mut added_time = Duration::new(0, 0);
    //     //adjust new height start time
    //     {
    //         let tnow = unix_now();
    //         let old_start_time = self.start_time;
    //         let this_interval = tnow.checked_sub(old_start_time);
    //         let config_interval = Duration::from_millis(status.interval);
    //         info!(
    //             "Receive new status {} get new chain status h: {}, r: {}, cost time: {:?}",
    //             self, status_height, new_round, this_interval
    //         );

    //         if status_height == self.height
    //             && (this_interval.is_none() || this_interval.unwrap() < config_interval)
    //         {
    //             if let Some(this_interval) = this_interval {
    //                 added_time = config_interval - this_interval;
    //             } else {
    //                 added_time = config_interval + (old_start_time - tnow);
    //             }
    //         }
    //     }

    //     if !self
    //         .is_round_leader(status_height + 1, INIT_ROUND, &self.params.signer.address)
    //         .unwrap_or(false)
    //         || self.is_only_one_node()
    //     {
    //         self.change_state_step(status_height, new_round, Step::CommitWait);
    //         self.set_state_timeout(status_height, new_round, Step::CommitWait, added_time);
    //         return;
    //     }

    //     if self.deal_old_height_when_commited(status_height) {
    //         self.new_round_start_with_added_time(status_height + 1, INIT_ROUND, added_time);
    //     }
    // }

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
            if !self.is_only_one_node() && self.uniform_proc_new_view(height, round) {
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
                    if !self.params.issue_nil_block {
                        self.send_txlist_request(height - 1);
                    }
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
            Step::Propose | Step::ProposeWait => {
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
                        self.wrap_follower_proc_prevote(height, round);
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
            Step::PrecommitAuth => {}
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
            Step::Commit | Step::CommitWait => {
                /*when rebooting ,we did not know chain if is ready
                    if chain garantee that when I sent commit_block,
                    it can always issue block, no need for this.
                */
                self.do_commit_work(height, round);
            }
            Step::NewView => {
                self.uniform_newview_timeout_vote(height, round);
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
                    if let Ok(decode) = deserialize(&vec_out) {
                        let (vheight, vround, verified): (u64, u64, i8) = decode;
                        let status: VerifiedBlockStatus = verified.into();
                        trace!(
                            "load_wal_log {} LogType::VerifiedPropose status {:?}",
                            self,
                            status
                        );
                        if status.is_ok() {
                            self.unverified_msg.remove(&(vheight, vround));
                        } else {
                            self.clean_proposal_locked_info();
                        }
                    }
                }
                LogType::VerifiedBlock => {
                    //     if let Ok(decode) = deserialize(&vec_out) {
                    //         let (vheight, vround, verified, bytes): (u64, u64, i8, Vec<u8>) =
                    //             decode;
                    //         let status: VerifiedBlockStatus = verified.into();
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
                                self.votes.add(height, round, step, sender, &vote_msg);
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

    fn send_proof_request(&self) {
        //let msg: Message = MiscellaneousReq::new().into();
        //trace!("send req chain proof ");
        // self.pub_sender
        //     .send((
        //         routing_key!(Consensus >> MiscellaneousReq).into(),
        //         msg.try_into().unwrap(),
        //     ))
        //     .unwrap();
    }

    fn check_proposal_proof(&self) -> bool {
        true
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
                    if let Some(svrmsg) = svrmsg {
                        match svrmsg {
                            BftSvrMsg::Conf(config) => {
                                self.params.timer.set_total_duration(config.block_interval as u64);
                                let mut validators = Vec::new();
                                for v in config.validators {
                                    validators.push(Address::from(&v[..]));
                                }
                                self.auth_manage.receive_authorities_list(
                                                // this tob fixed 
                                                self.height as usize,
                                                &validators,
                                                &validators,
                                            );
                            },
                            BftSvrMsg::PProof(_pproof,tx) => {
                                let res = self.check_proposal_proof();
                                tx.send(res).unwrap();
                            }
                        }
                    }
                },

                cback = self.bft_channels.ctl_back_rx.recv() => {
                    if let Some(cback) = cback {
                        match cback {
                            CtlBackBftMsg::GetProposalRes(proposal) => {

                            },
                            CtlBackBftMsg::CheckProposalRes(height,round,res) => {
                                trace!(
                                    "Process {} recieve check proposal  res h: {}, r: {} res {}",
                                    self,
                                    height,
                                    round,
                                    res,
                                );
                                if !res {
                                    self.proposal = None;
                                } else {
                                    self.follower_proc_proposal(height, round);
                                }

                                self.follower_prevote_send(height, round);
                                self.change_state_step(height, round, Step::PrevoteWait);
                                self.set_state_timeout(
                                    height,
                                    round,
                                    Step::PrevoteWait,
                                    self.proposal_interval_round_multiple(round),
                                );
                            }
                            CtlBackBftMsg::CommitBlockRes => {

                            }
                        }
                    }
                },
                net_msg = self.bft_channels.net_back_rx.recv() => {
                    if let Some(net_msg) = net_msg {
                        //self.process();
                    }

                },
                tminfo = self.bft_channels.timer_back_rx.recv() => {
                    if let Some(tminfo) = tminfo {
                        self.timeout_process(&tminfo);
                    }
                }
            }
        }
    }

    fn send_txlist_request(&self, _height: u64) {
        self.bft_channels
            .to_ctl_tx
            .send(BftToCtlMsg::GetProposalReq);

        trace!("Send txlist request {} ", _height);
    }
}
