use bincode;
use cita_cloud_proto::common::Hash;
use cita_cloud_proto::common::ProposalWithProof;
use cita_cloud_proto::consensus::{
    consensus_service_server::ConsensusService, ConsensusConfiguration,
};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use crate::voteset::{Proposal};
use crate::crypto::{pubkey_to_address, CreateKey, Sign, Signature, SIGNATURE_BYTES_LEN};
use cita_types::H256;

#[derive(Debug)]
pub enum BftSvrMsg {
    Conf(ConsensusConfiguration),
    PProof(ProposalWithProof,oneshot::Sender<bool>),
}

#[derive(Debug)]
pub enum BftToCtlMsg {
    GetProposalReq,
    CheckProposalReq(Vec<u8>),
    CommitBlock(ProposalWithProof),
}

#[derive(Debug)]
pub enum CtlBackBftMsg {
    GetProposalRes(Vec<u8>),
    CheckProposalRes(bool),
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
    pub fn set(&mut self,proposal: CompactProposal ,sig:Signature) -> Self {
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
    pub raw_proposal : Proposal,
}

impl CompactProposal {
    pub fn new(h:u64,r:u64,phash:H256) -> Self {
        let mut cp = CompactProposal::default();
        cp.height = h;
        cp.round = r;
        cp.raw_proposal.phash = phash;
    }

    pub fn new_with_proposal(h:u64,r:u64,p: Proposal) -> Self {
        let mut cp = CompactProposal::default();
        cp.height = h;
        cp.round = r;
        cp.raw_proposal = p;
    }

    pub fn set_raw_proposal(&mut self,p :Proposal) {
        self.raw_proposal = p;
    }
}

impl Into<Vec<u8>> for CompactProposal {
    fn into(self) -> Vec<u8> {
        bincode::serialize(&self).unwrap()
    }
}
