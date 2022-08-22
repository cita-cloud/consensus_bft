use crate::types::H256;
use crate::util::hash_msg;
use crate::voteset::Proposal;
use cita_cloud_proto::common::{ConsensusConfiguration, ProposalWithProof, StatusCode};
use cita_types::Address;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

macro_rules! impl_from {
    ($myty : ty) => {
        impl From<&$myty> for Vec<u8> {
            fn from(v: &$myty) -> Self {
                bincode::serialize(v).unwrap()
            }
        }
    };
}
#[derive(Debug)]
pub enum BftSvrMsg {
    Conf(ConsensusConfiguration),
    PProof(ProposalWithProof, oneshot::Sender<bool>),
}

#[derive(Debug)]
pub enum BftToCtlMsg {
    GetProposalReq,
    CheckProposalReq(u64, u64, Vec<u8>),
    CommitBlock(ProposalWithProof),
}

#[derive(Debug)]
pub enum CtlBackBftMsg {
    GetProposal(StatusCode, u64, Vec<u8>),
    CheckProposal(u64, u64, bool),
    CommitBlock(ConsensusConfiguration),
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, Default)]
pub struct Vote {
    pub sender: Vec<u8>,
    pub proposal: Vec<u8>,
    pub signature: Vec<u8>,
}

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct SignedNetworkProposal {
    pub proposal: NetworkProposal,
    pub sig: Vec<u8>,
    pub sender: Address,
}

impl SignedNetworkProposal {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn set(&mut self, proposal: NetworkProposal, sig: Vec<u8>, sender: Address) {
        self.proposal = proposal;
        self.sig = sig;
        self.sender = sender;
    }
}
#[derive(Serialize, Deserialize, Clone, Default)]
pub struct NetworkProposal {
    pub height: u64,
    pub round: u64,
    pub vote_proposal: Proposal,
    pub raw_proposal: Vec<u8>,
}

impl NetworkProposal {
    pub fn new(h: u64, r: u64, raw: Vec<u8>) -> Self {
        let vote_proposal = Proposal {
            phash: hash_msg(&raw),
            lock_round: None,
            lock_votes: None,
        };
        NetworkProposal {
            height: h,
            round: r,
            vote_proposal,
            raw_proposal: raw,
        }
    }

    pub fn new_with_proposal(h: u64, r: u64, p: Proposal) -> Self {
        Self {
            height: h,
            round: r,
            vote_proposal: p,
            raw_proposal: vec![],
        }
    }

    pub fn set_raw_proposal(&mut self, p: Vec<u8>) {
        self.raw_proposal = p;
    }
}

pub enum VoteMsgType {
    Noop,
    Proposal,
    Prevote,
    Precommit,
    NewView,
    LeaderPrevote,
    LeaderPrecommit,
}

impl From<&str> for VoteMsgType {
    fn from(s: &str) -> Self {
        match s {
            "proposal" => Self::Proposal,
            "prevote" => Self::Prevote,
            "precommit" => Self::Precommit,
            "newview" => Self::NewView,
            "lprevote" => Self::LeaderPrevote,
            "lprecommit" => Self::LeaderPrecommit,
            _ => Self::Noop,
        }
    }
}

impl From<VoteMsgType> for &str {
    fn from(v: VoteMsgType) -> Self {
        match v {
            VoteMsgType::Proposal => "proposal",
            VoteMsgType::Prevote => "prevote",
            VoteMsgType::Precommit => "precommit",
            VoteMsgType::LeaderPrevote => "lprevote",
            VoteMsgType::LeaderPrecommit => "lprecommit",
            VoteMsgType::NewView => "newview",
            VoteMsgType::Noop => "noop",
        }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, PartialOrd, Eq, Ord, Clone, Copy, Hash)]
pub enum Step {
    Propose,
    ProposeWait,
    Prevote,
    PrevoteWait,
    Precommit,
    PrecommitWait,
    Commit,
    CommitWait,
    NewView,
    NewViewRes,
}

impl Default for Step {
    fn default() -> Step {
        Step::Propose
    }
}

impl From<u8> for Step {
    fn from(s: u8) -> Step {
        match s {
            0 => Step::Propose,
            1 => Step::ProposeWait,
            2 => Step::Prevote,
            3 => Step::PrevoteWait,
            4 => Step::Precommit,
            5 => Step::PrecommitWait,
            6 => Step::Commit,
            7 => Step::CommitWait,
            8 => Step::NewView,
            9 => Step::NewViewRes,
            _ => panic!("Invalid step."),
        }
    }
}

impl ::std::fmt::Display for Step {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        write!(f, "{:?}", *self)
    }
}
#[derive(Serialize, Deserialize, Clone, Default, Debug)]
pub struct FollowerVote {
    pub height: u64,
    pub round: u64,
    pub step: Step,
    pub hash: Option<H256>,
}
#[derive(Serialize, Deserialize, Clone, Default, Debug)]
pub struct SignedFollowerVote {
    pub vote: FollowerVote,
    pub sig: Vec<u8>,
}
#[derive(Serialize, Deserialize, Clone, Default, Debug)]
pub struct LeaderVote {
    pub height: u64,
    pub round: u64,
    pub step: Step,
    pub hash: Option<H256>,
    pub votes: Vec<SignedFollowerVote>,
}

impl LeaderVote {
    pub fn new(height: u64, round: u64, step: Step) -> Self {
        Self {
            height,
            round,
            step,
            ..Default::default()
        }
    }
}

impl_from!(Vote);
impl_from!(SignedNetworkProposal);
impl_from!(NetworkProposal);
impl_from!(FollowerVote);
impl_from!(SignedFollowerVote);
impl_from!(LeaderVote);
