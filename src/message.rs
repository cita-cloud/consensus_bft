use crate::crypto::{pubkey_to_address, CreateKey, Sign, Signature, SIGNATURE_BYTES_LEN};
use crate::voteset::Proposal;
use bincode;
use cita_cloud_proto::common::Hash;
use cita_cloud_proto::common::ProposalWithProof;
use cita_cloud_proto::consensus::{
    consensus_service_server::ConsensusService, ConsensusConfiguration,
};
use cita_types::H256;
use hashable::Hashable;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

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
    GetProposalRes(u64, Vec<u8>),
    CheckProposalRes(u64, u64, bool),
    CommitBlockRes,
}

#[derive(Serialize, Deserialize, PartialEq, Clone, Default)]
pub struct Vote {
    pub sender: Vec<u8>,
    pub proposal: Vec<u8>,
    pub signature: Vec<u8>,
}

impl Vote {
    pub fn new() -> Self {
        Vote::default()
    }
}

impl Into<Vec<u8>> for Vote {
    fn into(self) -> Vec<u8> {
        bincode::serialize(&self).unwrap()
    }
}

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct SignedNetworkProposal {
    pub proposal: NetworkProposal,
    pub sig: Signature,
}

impl SignedNetworkProposal {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn set(&mut self, proposal: NetworkProposal, sig: Signature) {
        self.proposal = proposal;
        self.sig = sig;
    }
}

impl Into<Vec<u8>> for SignedNetworkProposal {
    fn into(self) -> Vec<u8> {
        bincode::serialize(&self).unwrap()
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
        let mut cp = NetworkProposal::default();
        cp.height = h;
        cp.round = r;
        let phash = raw.crypt_hash();
        cp.raw_proposal = raw;
        cp.vote_proposal.phash = phash;
        cp
    }

    pub fn new_with_proposal(h: u64, r: u64, p: Proposal) -> Self {
        let mut cp = NetworkProposal::default();
        cp.height = h;
        cp.round = r;
        cp.vote_proposal = p;
        cp
    }

    pub fn set_raw_proposal(&mut self, p: Vec<u8>) {
        self.raw_proposal = p;
    }
}

impl Into<Vec<u8>> for NetworkProposal {
    fn into(self) -> Vec<u8> {
        bincode::serialize(&self).unwrap()
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

impl Into<&str> for VoteMsgType {
    fn into(self) -> &'static str {
        match self {
            Self::Proposal => "proposal",
            Self::Prevote => "prevote",
            Self::Precommit => "precommit",
            Self::LeaderPrevote => "lprevote",
            Self::LeaderPrecommit => "lprecommit",
            Self::NewView => "newview",
            Self::Noop => "noop",
        }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, PartialOrd, Eq, Ord, Clone, Copy, Hash)]
pub enum Step {
    Propose,
    ProposeAuth,
    ProposeWait,
    Prevote,
    PrevoteWait,
    Precommit,
    PrecommitWait,
    Commit,
    CommitPending,
    CommitWait,
    NewView,
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
            1 => Step::ProposeAuth,
            2 => Step::ProposeWait,
            3 => Step::Prevote,
            4 => Step::PrevoteWait,
            5 => Step::Precommit,
            6 => Step::PrecommitWait,
            7 => Step::Commit,
            8 => Step::CommitPending,
            9 => Step::CommitWait,
            10 => Step::NewView,
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

impl Into<Vec<u8>> for FollowerVote {
    fn into(self) -> Vec<u8> {
        bincode::serialize(&self).unwrap()
    }
}

#[derive(Serialize, Deserialize, Clone, Default, Debug)]
pub struct SignedFollowerVote {
    pub vote: FollowerVote,
    pub sig: Signature,
}

impl Into<Vec<u8>> for SignedFollowerVote {
    fn into(self) -> Vec<u8> {
        bincode::serialize(&self).unwrap()
    }
}

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct LeaderVote {
    pub height: u64,
    pub round: u64,
    pub hash: Option<H256>,
    pub votes: Vec<SignedFollowerVote>,
}

impl Into<Vec<u8>> for LeaderVote {
    fn into(self) -> Vec<u8> {
        bincode::serialize(&self).unwrap()
    }
}
