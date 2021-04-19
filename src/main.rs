mod cita_bft;
mod config;
mod message;
mod params;
mod voteset;
mod votetime;
mod wal;

use cita_crypto as crypto;
use cita_types as types;
#[macro_use]
use cita_logger as logger;

#[macro_use]

#[macro_use]
use util;

use message::{BftSvrMsg, BftToCtlMsg, CtlBackBftMsg};
use util::{micro_service_init, set_panic_handler};


use logger::{debug, error, info, warn};
use tokio::sync::mpsc;

// #[cfg(feature = "timestamp_test")]
// use cita_bft::SignSymbol;
// #[cfg(feature = "timestamp_test")]
// use cita_bft::TimeOffset;
// use clap::App;
// #[cfg(feature = "timestamp_test")]
// use std::cmp::Ordering;
// use std::{env, thread};

use crate::cita_bft::{Bft, BftChannls};
use crate::params::{BftParams, PrivateKey};
use crate::votetime::WaitTimer;
use clap::Clap;
use git_version::git_version;

use cita_cloud_proto::common::{
    Empty, Proposal as ProtoProposal, ProposalWithProof, SimpleResponse,
};
use cita_cloud_proto::consensus::consensus_service_server::ConsensusServiceServer;
use cita_cloud_proto::consensus::{
    consensus_service_server::ConsensusService, ConsensusConfiguration,
};
use cita_cloud_proto::controller::consensus2_controller_service_client::Consensus2ControllerServiceClient;
use cita_cloud_proto::network::network_msg_handler_service_server::NetworkMsgHandlerService;
use cita_cloud_proto::network::network_msg_handler_service_server::NetworkMsgHandlerServiceServer;
use cita_cloud_proto::network::network_service_client::NetworkServiceClient;
use cita_cloud_proto::network::{NetworkMsg, RegisterInfo};
use tonic::transport::channel::Channel;
use tonic::{transport::Server, Request};

type ControllerClient = Consensus2ControllerServiceClient<Channel>;
type NetworkClient = NetworkServiceClient<Channel>;

struct BftSvr {
    to_bft_tx: mpsc::UnboundedSender<BftSvrMsg>,
}

impl BftSvr {
    pub fn new(to_bft_tx: mpsc::UnboundedSender<BftSvrMsg>) -> Self {
        BftSvr { to_bft_tx }
    }
}

#[tonic::async_trait]
impl ConsensusService for BftSvr {
    async fn reconfigure(
        &self,
        request: tonic::Request<ConsensusConfiguration>,
    ) -> std::result::Result<tonic::Response<SimpleResponse>, tonic::Status> {
        let config = request.into_inner();
        self.to_bft_tx.send(BftSvrMsg::Conf(config)).unwrap();
        let reply = SimpleResponse { is_success: true };
        Ok(tonic::Response::new(reply))
    }

    async fn check_block(
        &self,
        request: tonic::Request<ProposalWithProof>,
    ) -> std::result::Result<tonic::Response<SimpleResponse>, tonic::Status> {
        let pp = request.into_inner();
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.to_bft_tx.send(BftSvrMsg::PProof(pp, tx)).unwrap();
        let res = rx.await.unwrap();
        let reply = SimpleResponse { is_success: res };
        Ok(tonic::Response::new(reply))
    }
}

struct BftToCtl {
    //ctr_client: ControllerClient,
    ctr_port: u16,
    to_ctl_rx: mpsc::UnboundedReceiver<BftToCtlMsg>,
    back_bft_tx: mpsc::UnboundedSender<CtlBackBftMsg>,
}

impl BftToCtl {
    pub fn new(
        ctr_port: u16,
        to_ctl_rx: mpsc::UnboundedReceiver<BftToCtlMsg>,
        back_bft_tx: mpsc::UnboundedSender<CtlBackBftMsg>,
    ) -> Self {
        Self {
            ctr_port,
            to_ctl_rx,
            back_bft_tx,
        }
    }

    async fn reconnect(ctr_port: u16) -> ControllerClient {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        let ctl_addr = format!("http://127.0.0.1:{}", ctr_port);
        info!("connecting to controller...");
        let client = loop {
            interval.tick().await;
            match ControllerClient::connect(ctl_addr.clone()).await {
                Ok(client) => break client,
                Err(e) => {
                    debug!("connect to controller failed: `{}`", e);
                }
            }
            debug!("Retrying to connect controller");
        };
        info!("connecting to controller success");
        client
    }

    // Connect to the controller. Retry on failure.
    async fn run(mut b2c: BftToCtl) {
        let mut client = Self::reconnect(b2c.ctr_port).await;
        while let Some(to_msg) = b2c.to_ctl_rx.recv().await {
            warn!("consensus to ctrl get {:?}", to_msg);
            match to_msg {
                BftToCtlMsg::GetProposalReq => {
                    let request = tonic::Request::new(Empty {});
                    let response = client
                        .get_proposal(request)
                        .await
                        .map(|resp| resp.into_inner());
                    //.map_err(|e| e.into());
                    if let Ok(res) = response {
                        let msg = CtlBackBftMsg::GetProposalRes(res.height, res.data);
                        b2c.back_bft_tx.send(msg).unwrap();
                    }
                }

                BftToCtlMsg::CheckProposalReq(height, r, raw) => {
                    let request = tonic::Request::new(ProtoProposal { height, data: raw });
                    let response = client
                        .check_proposal(request)
                        .await
                        .map(|resp| resp.into_inner().is_success);
                    //.map_err(|e| e.into());
                    if let Ok(res) = response {
                        let msg = CtlBackBftMsg::CheckProposalRes(height, r, res);
                        b2c.back_bft_tx.send(msg).unwrap();
                    }
                }
                BftToCtlMsg::CommitBlock(height, pproff) => {
                    let request = tonic::Request::new(pproff);
                    let response = client.commit_block(request).await.map(|_resp| ());
                    //.map_err(|e| e.into());
                    if let Ok(_) = response {
                        let msg = CtlBackBftMsg::CommitBlockRes(height);
                        b2c.back_bft_tx.send(msg).unwrap();
                    }
                }
            }
        }
        error!("bft to controller channel closed");
    }
}

struct BftToNet {
    net_port: u16,
    self_port: String,
    to_net_rx: mpsc::UnboundedReceiver<NetworkMsg>,
}

impl BftToNet {
    pub fn new(
        net_port: u16,
        self_port: String,
        to_net_rx: mpsc::UnboundedReceiver<NetworkMsg>,
    ) -> Self {
        BftToNet {
            net_port,
            self_port,
            to_net_rx,
        }
    }

    async fn reconnect(net_port: u16) -> NetworkClient {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        let net_addr = format!("http://127.0.0.1:{}", net_port);
        info!("connecting to network...");
        let client = loop {
            interval.tick().await;
            match NetworkClient::connect(net_addr.clone()).await {
                Ok(client) => break client,
                Err(e) => {
                    debug!("connect to network failed: `{}`", e);
                }
            }
            debug!("Retrying to connect network");
        };
        client
    }

    async fn run(mut b2n: BftToNet) {
        let mut client = Self::reconnect(b2n.net_port).await;
        info!("connecting to network success");
        loop {
            let request = Request::new(RegisterInfo {
                module_name: "consensus".to_owned(),
                hostname: "127.0.0.1".to_owned(),
                port: b2n.self_port.clone(),
            });

            let response = client.register_network_msg_handler(request).await.unwrap();
            if response.into_inner().is_success {
                break;
            }
        }

        while let Some(msg) = b2n.to_net_rx.recv().await {
            let origin = msg.origin;
            let request = tonic::Request::new(msg);
            if origin == 0 {
                let resp = client.broadcast(request).await;
                if let Err(e) = resp {
                    info!("net client broadcast error {:?}", e);
                }
            } else {
                let resp = client.send_msg(request).await;
                if let Err(e) = resp {
                    info!("net client send_msg error {:?}", e);
                }
            }
        }
        error!("bft to net channel closed");
    }
}
struct NetToBft {
    to_bft_tx: mpsc::UnboundedSender<NetworkMsg>,
}

impl NetToBft {
    pub fn new(to_bft_tx: mpsc::UnboundedSender<NetworkMsg>) -> Self {
        NetToBft { to_bft_tx }
    }
}

#[tonic::async_trait]
impl NetworkMsgHandlerService for NetToBft {
    async fn process_network_msg(
        &self,
        request: tonic::Request<NetworkMsg>,
    ) -> std::result::Result<tonic::Response<SimpleResponse>, tonic::Status> {
        let msg = request.into_inner();
        if msg.module != "consensus" {
            Err(tonic::Status::invalid_argument("wrong module"))
        } else {
            info!("get netmsg {:?}", msg);
            self.to_bft_tx.send(msg).unwrap();
            let reply = SimpleResponse { is_success: true };
            Ok(tonic::Response::new(reply))
        }
    }
}

const GIT_VERSION: &str = git_version!(
    args = ["--tags", "--always", "--dirty=-modified"],
    fallback = "unknown"
);
const GIT_HOMEPAGE: &str = "https://github.com/cita-cloud/consensus_bft";

/// This doc string acts as a help message when the user runs '--help'
/// as do all doc strings on fields
#[derive(Clap)]
#[clap(version = "0.1.0", author = "Rivtower Technologies.")]
struct Opts {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

#[derive(Clap)]
enum SubCommand {
    /// print information from git
    #[clap(name = "git")]
    GitInfo,
    /// run this service
    #[clap(name = "run")]
    Run(RunOpts),
}

/// A subcommand for run
#[derive(Clap)]
struct RunOpts {
    /// Sets grpc port of this service.
    #[clap(short = 'p', long = "port", default_value = "50001")]
    grpc_port: String,
}

#[tokio::main]
async fn run(opts: RunOpts) {
    ::std::env::set_var("RUST_BACKTRACE", "full");
    ::std::env::set_var("DATA_PATH", "./data/");
    info!("start consensus bft");

    let buffer = std::fs::read_to_string("consensus-config.toml")
        .unwrap_or_else(|err| panic!("Error while loading config: [{}]", err));
    let config = config::BftConfig::new(&buffer);

    let (bft_tx, bft_rx) = mpsc::unbounded_channel();
    let bft_svr = BftSvr::new(bft_tx);

    let (to_ctl_tx, to_ctl_rx) = mpsc::unbounded_channel();
    let (ctl_back_tx, ctl_back_rx) = mpsc::unbounded_channel();
    let b2c = BftToCtl::new(config.controller_port, to_ctl_rx, ctl_back_tx);

    tokio::spawn(async move {
        BftToCtl::run(b2c).await;
    });

    let (to_net_tx, to_net_rx) = mpsc::unbounded_channel();
    let b2n = BftToNet::new(config.network_port, opts.grpc_port.clone(), to_net_rx);
    tokio::spawn(async move {
        BftToNet::run(b2n).await;
    });

    let (net_back_tx, net_back_rx) = mpsc::unbounded_channel();
    let n2b = NetToBft::new(net_back_tx);

    let (to_timer_tx, to_timer_rx) = mpsc::unbounded_channel();
    let (timer_back_tx, timer_back_rx) = mpsc::unbounded_channel();

    tokio::spawn(async move {
        let mut wt = WaitTimer::new(to_timer_rx, timer_back_tx);
        wt.start().await;
    });

    let bc = BftChannls {
        to_bft_rx: bft_rx,
        to_ctl_tx,
        ctl_back_rx,
        to_net_tx,
        net_back_rx,
        to_timer_tx,
        timer_back_rx,
    };

    let sk = PrivateKey::new(&config.sk);
    let bft_params = BftParams::new(&sk);
    let mut bft = Bft::new(bft_params, bc);

    tokio::spawn(async move {
        bft.start().await;
    });

    let addr_str = format!("127.0.0.1:{}", opts.grpc_port);
    let addr = addr_str.parse().unwrap();
    let _ = Server::builder()
        .add_service(ConsensusServiceServer::new(bft_svr))
        .add_service(NetworkMsgHandlerServiceServer::new(n2b))
        .serve(addr)
        .await;
}

fn main() {
    micro_service_init!("consensus_bft", "CITA-CLOUD:consensus:bft", false);

    let opts: Opts = Opts::parse();
    // You can handle information about subcommands by requesting their matches by name
    // (as below), requesting just the name used, or both at the same time
    match opts.subcmd {
        SubCommand::GitInfo => {
            println!("git version: {}", GIT_VERSION);
            println!("homepage: {}", GIT_HOMEPAGE);
        }
        SubCommand::Run(opts) => {
            run(opts);
        }
    }
}
