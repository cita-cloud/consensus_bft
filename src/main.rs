mod authority_manage;
mod cita_bft;
mod config;
mod error;
mod health_check;
mod message;
mod panic_hook;
mod params;
mod util;
mod voteset;
mod votetime;

use cita_types as types;

use message::{BftSvrMsg, BftToCtlMsg, CtlBackBftMsg};
use panic_hook::set_panic_handler;
use tokio::task;

use log::{debug, error, info, warn};
use tokio::sync::mpsc;

use crate::cita_bft::{Bft, BftChannls};
use crate::params::BftParams;
use crate::votetime::WaitTimer;
use clap::Parser;

use crate::config::BftConfig;
use crate::health_check::HealthCheckServer;
use crate::util::{crypto_client, init_grpc_client, CLIENT_NAME};
use cita_cloud_proto::client::{
    ClientOptions, ControllerClientTrait, CryptoClientTrait, InterceptedSvc, NetworkClientTrait,
};
use cita_cloud_proto::common::{
    ConsensusConfiguration, Empty, Proposal as ProtoProposal, ProposalWithProof, StatusCode,
};
use cita_cloud_proto::consensus::consensus_service_server::{
    ConsensusService, ConsensusServiceServer,
};
use cita_cloud_proto::controller::consensus2_controller_service_client::Consensus2ControllerServiceClient;
use cita_cloud_proto::health_check::health_server::HealthServer;
use cita_cloud_proto::network::network_msg_handler_service_server::NetworkMsgHandlerService;
use cita_cloud_proto::network::network_msg_handler_service_server::NetworkMsgHandlerServiceServer;
use cita_cloud_proto::network::network_service_client::NetworkServiceClient;
use cita_cloud_proto::network::{NetworkMsg, RegisterInfo};
use cita_cloud_proto::retry::RetryClient;
use cloud_util::metrics::{run_metrics_exporter, MiddlewareLayer};
use std::time::Duration;
use tonic::transport::Server;

type ControllerClient = RetryClient<Consensus2ControllerServiceClient<InterceptedSvc>>;
type NetworkClient = RetryClient<NetworkServiceClient<InterceptedSvc>>;

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
    connect_interval: u64,
    to_ctl_rx: mpsc::UnboundedReceiver<BftToCtlMsg>,
    back_bft_tx: mpsc::UnboundedSender<CtlBackBftMsg>,
}

impl BftToCtl {
    pub fn new(
        ctr_port: u16,
        connect_interval: u64,
        to_ctl_rx: mpsc::UnboundedReceiver<BftToCtlMsg>,
        back_bft_tx: mpsc::UnboundedSender<CtlBackBftMsg>,
    ) -> Self {
        Self {
            ctr_port,
            connect_interval,
            to_ctl_rx,
            back_bft_tx,
        }
    }

    async fn reconnect(&self) -> ControllerClient {
        let mut interval =
            tokio::time::interval(std::time::Duration::from_secs(self.connect_interval));
        let client_options = ClientOptions::new(
            CLIENT_NAME.to_string(),
            format!("http://127.0.0.1:{}", self.ctr_port),
        );
        info!("connecting to controller...");
        loop {
            interval.tick().await;
            match client_options.connect_controller() {
                Ok(retry_client) => {
                    info!("connecting to controller success");
                    return retry_client;
                }
                Err(e) => warn!("client init error: {:?}", &e),
            };
            debug!("Retrying to connect controller");
        }
    }

    // Connect to the controller. Retry on failure.
    async fn run(mut self) {
        let client = self.reconnect().await;
        while let Some(to_msg) = self.to_ctl_rx.recv().await {
            match to_msg {
                BftToCtlMsg::GetProposalReq => {
                    info!("consensus to controller : GetProposalReq ");
                    let res = client.get_proposal(Empty {}).await;

                    if let Ok(res) = res {
                        if let Some((status, proposal)) = res.status.zip(res.proposal) {
                            let msg =
                                CtlBackBftMsg::GetProposal(status, proposal.height, proposal.data);
                            self.back_bft_tx.send(msg).unwrap();
                        }
                    }
                }

                BftToCtlMsg::CheckProposalReq(height, r, raw) => {
                    info!(
                        "consensus to controller, CheckProposalReq h: {} r: {}",
                        height, r
                    );
                    let result = client
                        .check_proposal(ProtoProposal { height, data: raw })
                        .await;

                    let res = match result {
                        Ok(scode) => {
                            status_code::StatusCode::from(scode.code)
                                == status_code::StatusCode::Success
                        }
                        Err(status) => {
                            warn!("CheckProposalReq failed: {:?}", status);
                            false
                        }
                    };
                    let msg = CtlBackBftMsg::CheckProposal(height, r, res);
                    self.back_bft_tx.send(msg).unwrap();
                }

                BftToCtlMsg::CommitBlock(proposal_with_proof) => {
                    info!("consensus to controller : CommitBlock");
                    let client = client.clone();
                    let back_bft_tx = self.back_bft_tx.clone();
                    task::spawn(async move {
                        let res = client.commit_block(proposal_with_proof).await;

                        match res {
                            Ok(config) => {
                                if let Some((status, config)) = config.status.zip(config.config) {
                                    if status_code::StatusCode::from(status.code)
                                        == status_code::StatusCode::Success
                                    {
                                        back_bft_tx
                                            .send(CtlBackBftMsg::CommitBlock(config))
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
    connect_interval: u64,
    self_port: String,
    to_net_rx: mpsc::UnboundedReceiver<NetworkMsg>,
}

impl BftToNet {
    pub fn new(
        net_port: u16,
        connect_interval: u64,
        self_port: String,
        to_net_rx: mpsc::UnboundedReceiver<NetworkMsg>,
    ) -> Self {
        BftToNet {
            net_port,
            connect_interval,
            self_port,
            to_net_rx,
        }
    }

    async fn reconnect(&self) -> NetworkClient {
        let mut interval =
            tokio::time::interval(std::time::Duration::from_secs(self.connect_interval));
        let client_options = ClientOptions::new(
            CLIENT_NAME.to_string(),
            format!("http://127.0.0.1:{}", self.net_port),
        );
        info!("connecting to network...");
        loop {
            interval.tick().await;
            match client_options.connect_network() {
                Ok(retry_client) => {
                    return retry_client;
                }
                Err(e) => warn!("client init error: {:?}", &e),
            };
            debug!("Retrying to connect network");
        }
    }

    async fn run(mut self) {
        let client = self.reconnect().await;
        loop {
            let info = RegisterInfo {
                module_name: "consensus".to_owned(),
                hostname: "127.0.0.1".to_owned(),
                port: self.self_port.clone(),
            };

            let res = client.register_network_msg_handler(info).await.unwrap();
            if res.code == u32::from(status_code::StatusCode::Success) {
                info!("connecting to network success");
                break;
            }
        }

        while let Some(msg) = self.to_net_rx.recv().await {
            let origin = msg.origin;
            if origin == 0 {
                let resp = client.broadcast(msg).await;
                if let Err(e) = resp {
                    info!("net client broadcast error {:?}", e);
                }
            } else {
                let resp = client.send_msg(msg).await;
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
            debug!(
                "get network message module {:?} type {:?}",
                msg.module, msg.r#type
            );
            self.to_bft_tx.send(msg).unwrap();
            let reply = StatusCode {
                code: status_code::StatusCode::Success.into(),
            };
            Ok(tonic::Response::new(reply))
        }
    }
}

/// This doc string acts as a help message when the user runs '--help'
/// as do all doc strings on fields
#[derive(Parser)]
#[clap(version, author)]
struct Opts {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

#[derive(Parser)]
enum SubCommand {
    /// run this service
    #[clap(name = "run")]
    Run(RunOpts),
}

/// A subcommand for run
#[derive(Parser)]
struct RunOpts {
    /// Chain config path
    #[clap(short = 'c', long = "config", default_value = "config.toml")]
    config_path: String,
    /// log config path
    #[clap(short = 'l', long = "log", default_value = "consensus-log4rs.yaml")]
    log_file: String,
}

#[tokio::main]
async fn run(opts: RunOpts) {
    ::std::env::set_var("RUST_BACKTRACE", "full");
    ::std::env::set_var("DATA_PATH", "./data");
    ::std::env::set_var("WAL_PATH", "./data/wal");

    // read consensus-config.toml
    let config = BftConfig::new(&opts.config_path);

    // init log4rs
    log4rs::init_file(&opts.log_file, Default::default())
        .map_err(|e| println!("log init err: {}", e))
        .unwrap();

    let grpc_port = config.consensus_port.to_string();

    info!("grpc port of consensus_bft: {}", &grpc_port);

    let addr_str = format!("127.0.0.1:{}", &grpc_port);
    let addr = addr_str.parse().unwrap();

    init_grpc_client(&config);

    let mut interval = tokio::time::interval(Duration::from_secs(config.server_retry_interval));
    loop {
        interval.tick().await;
        // register endpoint
        {
            if let Ok(crypto_info) = crypto_client().get_crypto_info(Empty {}).await {
                if crypto_info.status.is_some() {
                    match status_code::StatusCode::from(crypto_info.status.unwrap()) {
                        status_code::StatusCode::Success => {
                            info!("crypto({}) is ready!", &crypto_info.name);
                            break;
                        }
                        status => warn!("get get_crypto_info failed: {:?}", status),
                    }
                }
            }
        }
        warn!("crypto not ready! Retrying");
    }

    let (to_bft_tx, to_bft_rx) = mpsc::unbounded_channel();
    let bft_svr = BftSvr::new(to_bft_tx);

    let (to_ctl_tx, to_ctl_rx) = mpsc::unbounded_channel();
    let (ctl_back_tx, ctl_back_rx) = mpsc::unbounded_channel();
    let b2c = BftToCtl::new(
        config.controller_port,
        config.server_retry_interval,
        to_ctl_rx,
        ctl_back_tx,
    );

    tokio::spawn(async move {
        b2c.run().await;
    });

    let (to_net_tx, to_net_rx) = mpsc::unbounded_channel();
    let b2n = BftToNet::new(
        config.network_port,
        config.server_retry_interval,
        grpc_port.clone(),
        to_net_rx,
    );
    tokio::spawn(async move {
        b2n.run().await;
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
        to_bft_rx,
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

    let layer = if config.enable_metrics {
        tokio::spawn(async move {
            run_metrics_exporter(config.metrics_port).await.unwrap();
        });

        Some(
            tower::ServiceBuilder::new()
                .layer(MiddlewareLayer::new(config.metrics_buckets))
                .into_inner(),
        )
    } else {
        None
    };

    info!("start consensus_bft grpc server");
    let _ = if layer.is_some() {
        info!("metrics on");
        Server::builder()
            .layer(layer.unwrap())
            .add_service(ConsensusServiceServer::new(bft_svr))
            .add_service(NetworkMsgHandlerServiceServer::new(n2b))
            .add_service(HealthServer::new(HealthCheckServer {}))
            .serve(addr)
            .await
    } else {
        info!("metrics off");
        Server::builder()
            .add_service(ConsensusServiceServer::new(bft_svr))
            .add_service(NetworkMsgHandlerServiceServer::new(n2b))
            .add_service(HealthServer::new(HealthCheckServer {}))
            .serve(addr)
            .await
    };
}

fn main() {
    set_panic_handler();
    let opts: Opts = Opts::parse();
    // You can handle information about subcommands by requesting their matches by name
    // (as below), requesting just the name used, or both at the same time
    match opts.subcmd {
        SubCommand::Run(opts) => {
            run(opts);
        }
    }
}
