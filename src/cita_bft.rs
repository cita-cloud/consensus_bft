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

use crate::crypto::{pubkey_to_address, CreateKey, Sign, Signature, SIGNATURE_BYTES_LEN};
use crate::message::{
    BftSvrMsg, BftToCtlMsg, CtlBackBftMsg, FollowerVote, LeaderVote, NetworkProposal,
    SignedFollowerVote, SignedNetworkProposal, Step, VoteMsgType,
};
use crate::params::BftParams;
use crate::types::{Address, H256};
use crate::voteset::{Proposal, ProposalCollector, VoteCollector, VoteSet};
use crate::votetime::TimeoutInfo;
use crate::wal::{LogType, Wal};
use authority_manage::AuthorityManage;
use bincode::deserialize;
use cita_cloud_proto::common::{
    ConsensusConfiguration, Proposal as ProtoProposal, ProposalWithProof,
};
use cita_cloud_proto::network::NetworkMsg;
use cita_directories::DataPath;
use engine::{unix_now, EngineError, Mismatch};
use hashable::Hashable;
use log::{debug, error, info, trace, warn};

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::convert::{From, Into};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
// #[macro_use]
// use lazy_static;

const INIT_HEIGHT: u64 = 1;
const INIT_ROUND: u64 = 0;

const MAX_PROPOSAL_TIME_COEF: u64 = 18;

const TIMEOUT_RETRANSE_MULTIPLE: u32 = 15;
const TIMEOUT_LOW_ROUND_FEED_MULTIPLE: u32 = 23;

const DEFAULT_TIME_INTERVAL: u64 = 3000;
type NilRound = (u64, u64);

#[derive(Debug, Clone, Copy, PartialEq)]
enum VerifiedProposalStatus {
    Ok,
    Err,
    Init,
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
    votes: VoteCollector,
    proposals: ProposalCollector,
    proposal: Option<H256>,
    self_proposal: BTreeMap<u64, H256>,
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
    leader_origins: BTreeMap<(u64, u64), u64>,

    bft_channels: BftChannls,
}

impl ::std::fmt::Debug for Bft {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        write!(
            f,
            "Bft {{ \
             h: {}, r: {}, stime: {:?}, s: {}, \
             , proposal: {:?},lock_round: {:?}, last_commit_round: {:?}, \
             is_consensus_node: {:?}, \
             }}",
            self.height,
            self.round,
            self.start_time,
            self.step,
            self.proposal,
            self.lock_round,
            self.last_commit_round,
            self.is_consensus_node,
        )
    }
}

impl ::std::fmt::Display for Bft {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        write!(
            f,
            "Bft {{ h: {}, r: {}, s: {}, stime: {:?} }}",
            self.height, self.round, self.step, self.start_time,
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
        let auth_manage = AuthorityManage::new();
        let is_consensus_node = auth_manage.validators.contains(&params.signer.address);
        Bft {
            params,
            height: 0,
            round: INIT_ROUND,
            nil_round: (INIT_ROUND, INIT_ROUND),
            step: Step::Propose,
            hash_proposals: lru_cache::LruCache::new(100),
            votes: VoteCollector::new(),
            proposals: ProposalCollector::new(),
            proposal: None,
            self_proposal: BTreeMap::new(),
            lock_round: None,
            wal_log: RefCell::new(Wal::create(&*logpath).unwrap()),
            send_filter: BTreeMap::new(),
            last_commit_round: None,
            start_time: unix_now(),
            auth_manage,
            is_consensus_node,
            leader_origins: BTreeMap::new(),
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
        if len == 1 && self.is_consensus_node {
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
            info!(" This round  proposer {:?} comming {:?}", proposer, address);
            Ok(false)
        }
    }

    pub fn pub_proposal(&mut self, signed_proposal: &SignedNetworkProposal) {
        self.send_raw_net_msg(VoteMsgType::Proposal.into(), 0, signed_proposal.clone());
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
                    //self.wal_save_state_step(height, round, Step::PrecommitWait);
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
                        "leader proc prevote errflag {} vote count {}",
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

    // fn get_faulty_limit_number(&self) -> u64 {
    //     self.auth_manage.validator_n() as u64 / 3
    // }

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
        let _lock_ok = false;

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
                            step: Step::Precommit,
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
            self.clean_self_proposal();
            self.clean_leader_origins();
            self.lock_round = None;
            self.last_commit_round = None;

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
                    step: Step::Precommit,
                    hash: Some(hash),
                    votes,
                };
                info!("gennerate proof ok {:?}", self);
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
                            data: raw_proposal,
                        }),
                        proof,
                    };
                    self.bft_channels
                        .to_ctl_tx
                        .send(BftToCtlMsg::CommitBlock(pproof))
                        .unwrap();
                    
                    //self.send_proposal_request();
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
        let (net_msg_type, origin) = {
            match step {
                Step::Prevote => (
                    VoteMsgType::Prevote,
                    self.leader_origins
                        .get(&(height, round))
                        .cloned()
                        .unwrap_or(0),
                ),
                Step::Precommit => (
                    VoteMsgType::Precommit,
                    self.leader_origins
                        .get(&(height, round))
                        .cloned()
                        .unwrap_or(0),
                ),
                Step::NewView => (VoteMsgType::NewView, 0),
                _ => (VoteMsgType::Noop, 0),
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
        self.send_raw_net_msg(net_msg_type.into(), origin, sv);
    }

    fn collect_votes(vote_set: &VoteSet, hash: H256) -> Vec<SignedFollowerVote> {
        let mut votes = Vec::new();
        //let mut senders = Vec::new();
        for sign_vote in vote_set.votes_by_sender.values() {
            if let Some(phash) = sign_vote.vote.hash {
                if phash != hash {
                    continue;
                }
                // senders.push(sender);
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

        match hash {
            Some(hash) if !hash.is_zero() => {
                let send_votes = self
                    .votes
                    .get_voteset(height, round, step)
                    .map(|vote_set| Self::collect_votes(&vote_set, hash));

                if let Some(votes) = send_votes {
                    if self.is_above_threshold(votes.len() as u64) {
                        let lv = LeaderVote {
                            height,
                            round,
                            step,
                            hash: Some(hash),
                            votes,
                        };
                        let lv_data: Vec<u8> = lv.into();
                        self.wal_save_message(height, LogType::QuorumVotes, &lv_data);
                        self.send_raw_net_msg(net_msg_type.into(), 0, lv_data)
                    } else {
                        error!("Can't be here, Why vote is Not enough {:?}", votes.len());
                    }
                }
            }
            _ => {
                self.send_raw_net_msg(net_msg_type.into(), 0, LeaderVote::new(height, round, step));
            }
        }
    }

    fn check_proposal_proof(&self, pproof: ProposalWithProof) -> bool {
        let phash = pproof.proposal.unwrap().data.crypt_hash();
        let lvote: LeaderVote = deserialize(&pproof.proof).unwrap_or_default();
        info!(
            "----- check_proposal_proof phash {:?} leader vote {:?}",
            phash, lvote
        );
        if !self.is_above_threshold(lvote.votes.len() as u64)
            && !self.is_above_threshold_old(lvote.votes.len() as u64)
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

            info!("----- check_proposal_proof sender {:?}", sender);
            if !self.is_validator(&sender) && !self.is_validator_old(&sender) {
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
        info!("----- check_proposal_proof ok");

        // if h > self.height {
        //     self.send_proposal_request();
        // }
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
        self.set_hrs(height, round, step);
    }

    fn wal_save_message(&self, height: u64, ltype: LogType, msg: &[u8]) -> Option<u64> {
        self.wal_log.borrow_mut().save(height, ltype, msg).ok()
    }

    fn wal_new_height(&self, height: u64) -> Option<u64> {
        self.wal_log.borrow_mut().set_height(height).ok()
    }

    fn check_leader_message(
        &mut self,
        lvote: &LeaderVote,
    ) -> Result<(u64, u64, Step, H256), EngineError> {
        let h = lvote.height;
        let r = lvote.round;
        let s = lvote.step;

        if h < self.height || (h == self.height && r < self.round) {
            return Err(EngineError::VoteMsgDelay(r as usize));
        }

        if h > self.height {
            return Err(EngineError::VoteMsgForth(h as usize));
        }

        if lvote.hash.is_none() || lvote.hash.unwrap().is_zero() {
            return Ok((h, r, s, H256::zero()));
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
                || s != sign_vote.vote.step
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
                h, r, s, sender, lvote.hash
            );

            let ret = self.votes.add(sender, &sign_vote);
            if !ret {
                debug!("Vote messsage add failed");
                return Err(EngineError::DoubleVote(sender));
            }
        }
        Ok((h, r, s, lvote.hash.unwrap()))
    }

    fn follower_handle_message(
        &mut self,
        net_msg: &NetworkMsg,
    ) -> Result<(u64, u64, H256), EngineError> {
        let lvote: LeaderVote =
            deserialize(&net_msg.msg).map_err(|_| EngineError::UnexpectedMessage)?;
        let ret = self.check_leader_message(&lvote);
        if let Ok((h, r, _s, hash)) = ret {
            self.wal_save_message(h, LogType::QuorumVotes, &net_msg.msg);
            return Ok((h, r, hash));
        }
        Err(ret.err().unwrap())
    }

    fn handle_newview(&mut self, net_msg: &NetworkMsg) -> Result<(u64, u64), EngineError> {
        let fvote: SignedFollowerVote =
            deserialize(&net_msg.msg).map_err(|_| EngineError::UnexpectedMessage)?;

        let h = fvote.vote.height;
        let r = fvote.vote.round;

        info!("handle_newview h {} r {}", h, r);
        if h < self.height {
            return Err(EngineError::VoteMsgDelay(h as usize));
        }
        // if h > self.height {
        //     self.send_proposal_request();
        // }
        let sender = Self::design_message(fvote.sig.clone(), fvote.vote.clone());
        info!("handle_newview sender {:?}", sender);
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
                info!("Pub newview for old round h {} r {}", h, r);
            }
            return Err(EngineError::VoteMsgDelay(r as usize));
        }

        /*bellow commit content is suit for when chain not syncing ,but consensus need
        process up */
        if (h > self.height && h < self.height + self.auth_manage.validator_n() as u64 + 1)
            || (h == self.height && r >= self.round)
        {
            info!(
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
        Err(EngineError::InvalidTimeInterval)
    }

    fn leader_handle_message(
        &mut self,
        net_msg: &NetworkMsg,
    ) -> Result<(u64, u64, Option<H256>), EngineError> {
        let fvote: SignedFollowerVote =
            deserialize(&net_msg.msg).map_err(|_| EngineError::UnexpectedMessage)?;
        info!("leader_handle_message {:?}", fvote);

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
                info!(
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
                    if h > self.height || r > self.round {
                        return Err(EngineError::VoteMsgForth(h as usize));
                    }
                    return Ok((h, r, fvote.vote.hash));
                }
                return Err(EngineError::DoubleVote(sender));
            }
        }
        Err(EngineError::NotProposer(Mismatch {
            expected: self.params.signer.address,
            found: sender,
        }))
    }

    fn follower_proc_proposal(&mut self, height: u64, round: u64) -> Option<bool> {
        let proposal = self.proposals.get_proposal(height, round);
        if let Some(proposal) = proposal {
            info!("proc proposal {} begin h: {}, r: {}", self, height, round);

            if proposal.is_default() {
                info!("proc proposal is default empty");
                return Some(false);
            }

            if !proposal.check(height, &self.auth_manage.validators) {
                warn!("Proc proposal check authorities error");
                return Some(false);
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
                self.clean_proposal_locked_info();
            }

            let res = self.hash_proposals.get_mut(&proposal.phash);
            match res {
                None => {
                    return None;
                }
                Some((_, res)) => match res {
                    VerifiedProposalStatus::Err => {
                        debug!("Proc proposal verified proposal error {:?}", proposal.phash);
                        return Some(false);
                    }
                    VerifiedProposalStatus::Init => {
                        debug!("Proc proposal verified proposal init {:?}", proposal.phash);
                        return None;
                    }
                    _ => {}
                },
            }
            self.proposal = Some(proposal.phash);
            return Some(true);
        }
        info!("Proc proposal not find proposal h {} r {}", height, round);
        None
    }

    fn send_proposal_verify_req(&self, height: u64, round: u64, raw_proposal: Vec<u8>) {
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

    fn check_proposal_hash(&mut self, hash: &H256) -> Option<bool> {
        if let Some(pro_status) = self.hash_proposals.get_mut(hash) {
            match pro_status.1 {
                VerifiedProposalStatus::Ok => {
                    return Some(true);
                }
                VerifiedProposalStatus::Err => {
                    return Some(false);
                }
                _ => {}
            }
        }
        None
    }

    fn handle_proposal(&mut self, net_msg: &NetworkMsg) -> Result<(u64, u64), EngineError> {
        let sign_proposal: SignedNetworkProposal =
            deserialize(&net_msg.msg).map_err(|_| EngineError::UnexpectedMessage)?;
        let height = sign_proposal.proposal.height;
        let round = sign_proposal.proposal.round;

        info!("handle_proposal h {} r {}", height, round);
        if self.proposals.get_proposal(height, round).is_some() {
            trace!("handle_proposal proposal recieved");
            return Err(EngineError::DoubleVote(Address::zero()));
        }

        let signature = sign_proposal.sig;
        if signature.len() != SIGNATURE_BYTES_LEN {
            return Err(EngineError::InvalidSignature);
        }

        if height < self.height
            || (height == self.height && round < self.round)
            || (height == self.height && round == self.round && self.step > Step::ProposeWait)
        {
            info!("Handle proposal {} get old proposal", self);
            return Err(EngineError::VoteMsgDelay(height as usize));
        } else if height == self.height && self.step == Step::CommitWait {
            info!(
                "Not handle proposal {} because conensus is ok in height",
                self
            );
            return Err(EngineError::VoteMsgForth(round as usize));
        }

        info!(
            "handle proposal {} hash {:?}",
            self, sign_proposal.proposal.vote_proposal.phash
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

        self.proposals
            .add(height, round, sign_proposal.proposal.vote_proposal.clone());

        self.leader_origins.insert((height, round), net_msg.origin);
        if height >= self.height && height < self.height + self.auth_manage.validator_n() as u64 + 1
        {
            let check_res = self.check_proposal_hash(&sign_proposal.proposal.vote_proposal.phash);
            debug!(
                "Handle proposal {} add proposal h: {}, r: {} check_res {:?}",
                self, height, round, check_res
            );

            let status = match check_res {
                Some(true) => VerifiedProposalStatus::Ok,
                Some(false) => VerifiedProposalStatus::Err,
                _ => {
                    self.send_proposal_verify_req(
                        height,
                        round,
                        sign_proposal.proposal.raw_proposal.clone(),
                    );
                    VerifiedProposalStatus::Init
                }
            };

            self.hash_proposals.insert(
                sign_proposal.proposal.vote_proposal.phash,
                (sign_proposal.proposal.raw_proposal, status),
            );

            if height == self.height && self.round == round {
                if check_res == Some(true) {
                    return Ok((height, round));
                } else {
                    return Err(EngineError::InvalidTxInProposal);
                }
            }
            return Err(EngineError::VoteMsgForth(height as usize));
        }
        Err(EngineError::InvalidTimeInterval)
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

    fn clean_self_proposal(&mut self) {
        self.self_proposal = self.self_proposal.split_off(&self.height);
    }

    fn clean_filter_info(&mut self) {
        self.send_filter.clear();
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
                let raw = self.hash_proposals.get_mut(&proposal.phash).cloned();
                if let Some((raw, _)) = raw {
                    self.proposal = Some(proposal.phash);
                    self.proposals
                        .add(self.height, self.round, proposal.clone());
                    let mut cp =
                        NetworkProposal::new_with_proposal(self.height, self.round, proposal);
                    cp.set_raw_proposal(raw);
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
            let hash = self.self_proposal.get(&self.height).to_owned();
            if hash.is_none() {
                info!("New proposal self_proposal is none {}", self);
                self.send_proposal_request();
                return false;
            }
            let hash = hash.unwrap();
            let raw = self.hash_proposals.get_mut(&hash);
            if raw.is_none() {
                info!("New proposal hash proposal is none {}", self);
                self.send_proposal_request();
                return false;
            }
            self.proposal = Some(*hash);
            let raw = raw.unwrap().0.to_owned();
            let p = Proposal::new(raw.crypt_hash());
            self.proposals.add(self.height, self.round, p.clone());
            let mut cp = NetworkProposal::new_with_proposal(self.height, self.round, p);
            cp.set_raw_proposal(raw);
            let sig = self.sign_msg(cp.clone());
            sign_prop.set(cp, sig);
            info!("New proposal proposal {:?}", self);
        }

        self.pub_proposal(&sign_prop);
        if save_flag {
            // let msg: Vec<u8> = sign_prop.into();
            // self.wal_save_message(self.height, LogType::Propose, &msg);
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
                let res = self.handle_proposal(&net_msg);
                warn!("------ net msg Proposal res {:?}", res);
                if let Ok((height, round)) = res {
                    if self.follower_proc_proposal(height, round).is_some() {
                        self.bundle_op_after_proposal(height, round);
                    }
                }
            }
            VoteMsgType::Prevote => {
                let res = self.leader_handle_message(&net_msg);
                warn!("------ net msg Prevote res {:?}", res);
                if let Ok((h, r, hash)) = res {
                    self.leader_proc_prevote(h, r, hash);
                }
            }
            VoteMsgType::Precommit => {
                let res = self.leader_handle_message(&net_msg);
                warn!("------ net msg Precommit res {:?}", res);
                if let Ok((h, r, hash)) = res {
                    self.leader_proc_precommit(h, r, hash);
                }
            }
            VoteMsgType::LeaderPrevote => {
                let res = self.follower_handle_message(&net_msg);
                warn!("------ net msg LeaderPrevote res {:?}", res);
                if let Ok((h, r, hash)) = res {
                    self.follower_proc_prevote(h, r, hash);
                }
            }
            VoteMsgType::LeaderPrecommit => {
                let res = self.follower_handle_message(&net_msg);
                warn!("------ net msg LeaderPrecommit res {:?}", res);
                if let Ok((h, r, hash)) = res {
                    self.follower_proc_precommit(h, r, Some(hash));
                }
            }
            VoteMsgType::NewView => {
                let res = self.handle_newview(&net_msg);
                warn!("------ net msg NewView res {:?}", res);
                if let Ok((h, r)) = res {
                    self.proc_new_view(h, r);
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

    fn new_round_start_with_added_time(
        &mut self,
        height: u64,
        round: u64,
        remaining_time: Duration,
    ) {
        self.change_state_step(height, round, Step::Propose);
        info!(
            "new_round_start_with_added_time added time {:?} self {}",
            remaining_time, self
        );

        info!("------- self address {:?}", self.params.signer.address);
        if round == INIT_ROUND {
            self.wal_new_height(height);
            self.start_time = unix_now() + remaining_time;
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
            tv += remaining_time;
        } else if self.follower_proc_proposal(height, round).is_some() {
            self.follower_prevote_send(height, round);
            step = Step::PrevoteWait;
            tv = self.proposal_interval_round_multiple(round);
        }

        self.change_state_step(height, round, step);
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
                        self.follower_proc_prevote(
                            height,
                            round,
                            self.proposal.unwrap_or_default(),
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
            Step::Commit | Step::CommitWait => {
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

    fn set_hrs(&mut self, h: u64, r: u64, s: Step) {
        self.height = h;
        self.round = r;
        self.step = s;
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
                    let prop = deserialize(&vec_out);
                    if prop.is_err() {
                        continue;
                    }
                    let prop: NetworkProposal = prop.unwrap();

                    let hash = prop.vote_proposal.phash;
                    self.hash_proposals
                        .insert(hash, (prop.raw_proposal, VerifiedProposalStatus::Ok));

                    self.proposals
                        .add(prop.height, prop.round, prop.vote_proposal);
                }

                LogType::QuorumVotes => {
                    let lvote = deserialize(&vec_out);
                    if lvote.is_err() {
                        continue;
                    }
                    let lvote = lvote.unwrap();
                    let ret = self.check_leader_message(&lvote);
                    if let Ok((h, r, s, hash)) = ret {
                        self.set_hrs(h, r, s);

                        if self
                            .is_round_leader(h, r, &self.params.signer.address)
                            .unwrap_or(false)
                        {
                            if s == Step::Prevote {
                                self.leader_proc_prevote(h, r, Some(hash));
                            } else {
                                self.leader_proc_precommit(h, r, Some(hash));
                            }
                        } else if s == Step::Prevote {
                            self.follower_proc_prevote(h, r, hash);
                        } else {
                            self.follower_proc_precommit(h, r, Some(hash));
                        }
                    }
                }
            }
        }
        trace!("load_wal_log ends");
    }

    fn recv_new_height_proposal(&mut self, h: u64, proposal: Vec<u8>) {
        if h < self.height {
            return;
        }
        let hash = proposal.crypt_hash();
        self.hash_proposals
            .insert(hash, (proposal, VerifiedProposalStatus::Ok));
        self.self_proposal.insert(h, hash);

        info!(
            "--- recv_new_height_proposal h: {:?} hash {:?} self {:?}",
            h, hash, self
        );

        if h == self.height
            && self.step == Step::ProposeWait
            && self
                .is_round_leader(self.height, self.round, &self.params.signer.address)
                .unwrap_or(false)
            && self.proposal.is_none()
        {
            self.leader_new_proposal(true);
        }
    }

    fn proc_commit_res(&mut self, config: ConsensusConfiguration) {
        let now = unix_now();
        info!(
            "--- now {:?} start time {:?} interval {} commint height {}",
            now,
            self.start_time,
            self.params.timer.get_total_duration(),
            h
        );

        if h < self.height {
            return;
        }

        let config_interval = Duration::from_millis(self.params.timer.get_total_duration());
        let remaining_time = (self.start_time + config_interval)
            .checked_sub(now)
            .unwrap_or_else(|| Duration::new(0, 0));
        let conf_height = config.height;
        self.set_config(config);

        if !self
            .is_round_leader(conf_height + 1, INIT_ROUND, &self.params.signer.address)
            .unwrap_or(false)
            || self.is_only_one_node()
        {
            self.change_state_step(conf_height, self.round, Step::CommitWait);
            self.set_state_timeout(conf_height, self.round, Step::CommitWait, remaining_time);
            return;
        }

        if self.deal_old_height_when_commited(conf_height) {
            self.new_round_start_with_added_time(conf_height + 1, INIT_ROUND, remaining_time);
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

    fn set_config(&mut self, config: ConsensusConfiguration) {
        self.params
            .timer
            .set_total_duration((config.block_interval * 1000) as u64);
        let mut validators = Vec::new();
        for v in config.validators {
            validators.push(Address::from(&v[..]));
        }
        self.auth_manage
            .receive_authorities_list(config.height as usize, &validators, &validators);

        if self.is_validator(&self.params.signer.address) {
            self.is_consensus_node = true;
            if self.is_round_leader(config.height + 1, INIT_ROUND, &self.params.signer.address).unwrap_or(false) {
                self.send_proposal_request();
            }
        } else {
            self.is_consensus_node = false;
        }
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
                                self.set_config(config);
                            },
                            BftSvrMsg::PProof(pproof,tx) => {
                                let res = self.check_proposal_proof(pproof);
                                tx.send(res).unwrap();

                            }
                        }
                    }
                },

                cback = self.bft_channels.ctl_back_rx.recv() => {
                    if let Some(cback) = cback {
                        match cback {
                            CtlBackBftMsg::GetProposalRes(height,proposal) => {
                                info!("recv to control back GetProposalRes height {:?}",height);
                                self.recv_new_height_proposal(height,proposal);
                            },
                            CtlBackBftMsg::CheckProposalRes(height,round,res) => {
                                trace!(
                                    "Process {} recieve check proposal  res h: {}, r: {} res {}",
                                    self,
                                    height,
                                    round,
                                    res,
                                );
                                let hash = self.proposals.get_proposal(height,round).map(|p| p.phash);
                                if let Some(hash) = hash {
                                    if height == self.height && round == self.round && self.step < Step::Prevote {

                                        if !res {
                                            self.proposal = None;
                                            if let Some(raw) = self.hash_proposals.get_mut(&hash) { raw.1=VerifiedProposalStatus::Err; }
                                        } else {
                                            let msg: Vec<u8> =
                                                self.hash_proposals.get_mut(&hash).map(|raw| {
                                                    raw.1=VerifiedProposalStatus::Ok;
                                                    raw.0.to_owned()
                                                }).unwrap_or_default();


                                            if !msg.is_empty() {
                                                let smsg:Vec<u8> = NetworkProposal::new(height,round,msg).into();
                                                self.wal_save_message(height,LogType::Propose,&smsg);
                                            }
                                            self.follower_proc_proposal(height, round);
                                        }
                                        self.bundle_op_after_proposal(height,round);
                                    }
                                }
                            }
                            CtlBackBftMsg::CommitBlockRes(config) => {
                                // send proposal request too close to leader's send
                                //self.send_proposal_request();
                                info!("recv to control back CommitBlockRes {:?}",config);
                                self.proc_commit_res(config);
                            }
                        }
                    }
                },
                net_msg = self.bft_channels.net_back_rx.recv() => {
                    if !self.is_consensus_node {
                        continue;
                    } else if let Some(net_msg) = net_msg {
                        info!("recv network back to bft msg module {:?} type {:?}",net_msg.module,net_msg.r#type);
                        self.process_network(net_msg);
                    }
                },
                tminfo = self.bft_channels.timer_back_rx.recv() => {
                    //info!("recv to timer back msg {:?}",tminfo);
                    if let Some(tminfo) = tminfo {
                        self.timeout_process(&tminfo);
                    }
                }
            }
        }
    }

    fn send_proposal_request(&self) {
        self.bft_channels
            .to_ctl_tx
            .send(BftToCtlMsg::GetProposalReq)
            .unwrap();
        trace!("send_proposal_request height {} ", self.height);
    }
}
