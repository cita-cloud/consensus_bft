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
    GetProposalRes(Vec<u8>),
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
pub struct CompactSignedProposal {
    pub proposal: CompactProposal,
    pub sig: Signature,
}

impl CompactSignedProposal {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn set(&mut self, proposal: CompactProposal, sig: Signature) {
        self.proposal = proposal;
        self.sig = sig;
    }
}

impl Into<Vec<u8>> for CompactSignedProposal {
    fn into(self) -> Vec<u8> {
        bincode::serialize(&self).unwrap()
    }
}

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct CompactProposal {
    pub height: u64,
    pub round: u64,
    pub raw_proposal: Vec<u8>,
    pub vote_proposal: Proposal,
}

impl CompactProposal {
    pub fn new(h: u64, r: u64, raw: Vec<u8>) -> Self {
        let mut cp = CompactProposal::default();
        cp.height = h;
        cp.round = r;
        let phash = raw.crypt_hash();
        cp.raw_proposal = raw;
        cp.vote_proposal.phash = phash;
        cp
    }

    pub fn new_with_proposal(h: u64, r: u64, p: Proposal) -> Self {
        let mut cp = CompactProposal::default();
        cp.height = h;
        cp.round = r;
        cp.vote_proposal = p;
        cp
    }

    pub fn set_raw_proposal(&mut self, p: Vec<u8>) {
        self.raw_proposal = p;
    }
}

impl Into<Vec<u8>> for CompactProposal {
    fn into(self) -> Vec<u8> {
        bincode::serialize(&self).unwrap()
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, PartialOrd, Eq, Ord, Clone, Copy, Hash)]
pub enum Step {
    Propose = 0,
    ProposeWait = 1,
    Prevote = 2,
    PrevoteWait = 3,
    PrecommitAuth = 4,
    Precommit = 5,
    PrecommitWait = 6,
    Commit = 7,
    CommitWait = 8,
    NewView = 9,
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
            4 => Step::PrecommitAuth,
            5 => Step::Precommit,
            6 => Step::PrecommitWait,
            7 => Step::Commit,
            8 => Step::CommitWait,
            9 => Step::NewView,
            _ => panic!("Invalid step."),
        }
    }
}

impl ::std::fmt::Display for Step {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        write!(f, "{:?}", *self)
    }
}
#[derive(Serialize, Deserialize, Clone, Default)]
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

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct SignedFollowerVote {
    pub vote: FollowerVote,
    pub sig: Signature,
}

impl Into<Vec<u8>> for SignedFollowerVote {
    fn into(self) -> Vec<u8> {
        bincode::serialize(&self).unwrap()
    }
}
