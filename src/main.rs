mod authority_manage;
mod cita_bft;
mod config;
mod error;
mod message;
mod panic_hook;
mod params;
mod util;
mod voteset;
mod votetime;
mod wal;

use cita_types as types;

use message::{BftSvrMsg, BftToCtlMsg, CtlBackBftMsg};
use panic_hook::set_panic_handler;
use tokio::task;

use log::{debug, error, info, warn};
use tokio::sync::mpsc;

use crate::cita_bft::{Bft, BftChannls};
use crate::params::BftParams;
use crate::votetime::WaitTimer;
use clap::Clap;
use git_version::git_version;

use crate::config::BftConfig;
use crate::util::{init_grpc_client, kms_client};
use cita_cloud_proto::common::{
    ConsensusConfiguration, Empty, Proposal as ProtoProposal, ProposalWithProof, StatusCode,
};
use cita_cloud_proto::consensus::consensus_service_server::{
    ConsensusService, ConsensusServiceServer,
};
use cita_cloud_proto::controller::consensus2_controller_service_client::Consensus2ControllerServiceClient;
use cita_cloud_proto::network::network_msg_handler_service_server::NetworkMsgHandlerService;
use cita_cloud_proto::network::network_msg_handler_service_server::NetworkMsgHandlerServiceServer;
use cita_cloud_proto::network::network_service_client::NetworkServiceClient;
use cita_cloud_proto::network::{NetworkMsg, RegisterInfo};
use std::time::Duration;
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
    ) -> std::result::Result<tonic::Response<StatusCode>, tonic::Status> {
        let config = request.into_inner();
        self.to_bft_tx.send(BftSvrMsg::Conf(config)).unwrap();
        let reply = StatusCode {
            code: status_code::StatusCode::Success.into(),
        };
        Ok(tonic::Response::new(reply))
    }

    async fn check_block(
        &self,
        request: tonic::Request<ProposalWithProof>,
    ) -> std::result::Result<tonic::Response<StatusCode>, tonic::Status> {
        let pp = request.into_inner();
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.to_bft_tx.send(BftSvrMsg::PProof(pp, tx)).unwrap();
        let res = rx.await.unwrap();
        let code = if res {
            status_code::StatusCode::Success.into()
        } else {
            status_code::StatusCode::ProposalCheckError.into()
        };
        Ok(tonic::Response::new(StatusCode { code }))
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
            match to_msg {
                BftToCtlMsg::GetProposalReq => {
                    info!("consensus to ctrl : GetProposalReq ");
                    let request = tonic::Request::new(Empty {});
                    let response = client
                        .get_proposal(request)
                        .await
                        .map(|resp| resp.into_inner());

                    if let Ok(res) = response {
                        if let Some((status, proposal)) = res.status.zip(res.proposal) {
                            let msg = CtlBackBftMsg::GetProposalRes(
                                status,
                                proposal.height,
                                proposal.data,
                            );
                            b2c.back_bft_tx.send(msg).unwrap();
                        }
                    }
                }

                BftToCtlMsg::CheckProposalReq(height, r, raw) => {
                    info!("consensus to ctrl, CheckProposalReq h {} r {}", height, r);
                    let request = tonic::Request::new(ProtoProposal { height, data: raw });
                    let response = client
                        .check_proposal(request)
                        .await
                        .map(|resp| resp.into_inner());

                    let res = match response {
                        Ok(scode) => {
                            status_code::StatusCode::from(scode.code)
                                == status_code::StatusCode::Success
                        }
                        Err(status) => {
                            warn!("CheckProposalReq failed: {:?}", status);
                            false
                        }
                    };
                    let msg = CtlBackBftMsg::CheckProposalRes(height, r, res);
                    b2c.back_bft_tx.send(msg).unwrap();
                }

                BftToCtlMsg::CommitBlock(pproff) => {
                    let mut client = client.clone();
                    let back_bft_tx = b2c.back_bft_tx.clone();
                    info!("consensus to ctrl : CommitBlock");
                    task::spawn(async move {
                        let request = tonic::Request::new(pproff);
                        let response = client.commit_block(request).await;

                        match response {
                            Ok(res) => {
                                let config = res.into_inner();
                                if let Some((status, config)) = config.status.zip(config.config) {
                                    if status_code::StatusCode::from(status.code)
                                        == status_code::StatusCode::Success
                                    {
                                        back_bft_tx
                                            .send(CtlBackBftMsg::CommitBlockRes(config))
                                            .unwrap();
                                    }
                                }
                            }
                            Err(e) => warn!("{:?}", e),
                        }
                    });
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
            if response.into_inner().code == u32::from(status_code::StatusCode::Success) {
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
    ) -> std::result::Result<tonic::Response<StatusCode>, tonic::Status> {
        let msg = request.into_inner();
        if msg.module != "consensus" {
            Err(tonic::Status::invalid_argument("wrong module"))
        } else {
            info!("get netmsg module {:?} type {:?}", msg.module, msg.r#type);
            self.to_bft_tx.send(msg).unwrap();
            let reply = StatusCode {
                code: status_code::StatusCode::Success.into(),
            };
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
    /// Chain config path
    #[clap(short = 'c', long = "config", default_value = "config.toml")]
    config_path: String,
}

#[tokio::main]
async fn run(opts: RunOpts) {
    ::std::env::set_var("RUST_BACKTRACE", "full");
    ::std::env::set_var("DATA_PATH", "./data");
    ::std::env::set_var("WAL_PATH", "./data/wal");

    // read consensus-config.toml
    let config = BftConfig::new(&opts.config_path);

    // init log4rs
    log4rs::init_file(&config.log_file, Default::default()).unwrap();
    info!("start consensus bft");

    let grpc_port = {
        if "50001" != opts.grpc_port {
            opts.grpc_port.clone()
        } else if config.consensus_port != 50001 {
            config.consensus_port.to_string()
        } else {
            "50001".to_string()
        }
    };
    info!("grpc port of this service: {}", &grpc_port);

    init_grpc_client(&config);

    let mut interval = tokio::time::interval(Duration::from_secs(config.server_retry_interval));
    loop {
        interval.tick().await;
        // register endpoint
        {
            if let Ok(crypto_info) = kms_client().get_crypto_info(Request::new(Empty {})).await {
                let inner = crypto_info.into_inner();
                if inner.status.is_some() {
                    match status_code::StatusCode::from(inner.status.unwrap()) {
                        status_code::StatusCode::Success => {
                            info!("kms({}) is ready!", &inner.name);
                            break;
                        }
                        status => warn!("get get_crypto_info failed: {:?}", status),
                    }
                }
            }
        }
        warn!("kms not ready! Retrying");
    }

    let (bft_tx, bft_rx) = mpsc::unbounded_channel();
    let bft_svr = BftSvr::new(bft_tx);

    let (to_ctl_tx, to_ctl_rx) = mpsc::unbounded_channel();
    let (ctl_back_tx, ctl_back_rx) = mpsc::unbounded_channel();
    let b2c = BftToCtl::new(config.controller_port, to_ctl_rx, ctl_back_tx);

    tokio::spawn(async move {
        BftToCtl::run(b2c).await;
    });

    let (to_net_tx, to_net_rx) = mpsc::unbounded_channel();
    let b2n = BftToNet::new(config.network_port, grpc_port.clone(), to_net_rx);
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

    let bft_params = BftParams::new(&config);
    let mut bft = Bft::new(bft_params, bc);

    tokio::spawn(async move {
        bft.start().await;
    });

    let addr_str = format!("127.0.0.1:{}", &grpc_port);
    let addr = addr_str.parse().unwrap();
    let _ = Server::builder()
        .add_service(ConsensusServiceServer::new(bft_svr))
        .add_service(NetworkMsgHandlerServiceServer::new(n2b))
        .serve(addr)
        .await;
}

fn main() {
    set_panic_handler();
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
