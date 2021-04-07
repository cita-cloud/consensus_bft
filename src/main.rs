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
use serde_derive;
#[macro_use]
use util;
use message::{BftSvrMsg, BftToCtlMsg, BftToNetMsg, CtlBackBftMsg};

use logger::{debug, error, info, trace, warn};
use tokio::sync::mpsc;

// #[cfg(feature = "timestamp_test")]
// use cita_bft::SignSymbol;
// #[cfg(feature = "timestamp_test")]
// use cita_bft::TimeOffset;
// use clap::App;
// #[cfg(feature = "timestamp_test")]
// use std::cmp::Ordering;
// use std::{env, thread};

use crate::cita_bft::{Bft, BftChannls, BftTurn};
use crate::params::{BftParams, PrivateKey};
use crate::votetime::WaitTimer;
use util::set_panic_handler;

use cita_cloud_proto::common::ProposalWithProof;
use cita_cloud_proto::common::SimpleResponse;
use cita_cloud_proto::consensus::consensus_service_server::ConsensusServiceServer;
use cita_cloud_proto::consensus::{
    consensus_service_server::ConsensusService, ConsensusConfiguration,
};
use cita_cloud_proto::controller::consensus2_controller_service_client::Consensus2ControllerServiceClient;
use cita_cloud_proto::network::network_msg_handler_service_server::NetworkMsgHandlerService;
use cita_cloud_proto::network::network_msg_handler_service_server::NetworkMsgHandlerServiceServer;
use cita_cloud_proto::network::network_service_client::NetworkServiceClient;
use cita_cloud_proto::network::NetworkMsg;
use cita_cloud_proto::network::RegisterInfo;
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
    ) -> Result<tonic::Response<SimpleResponse>, tonic::Status> {
        let pp = request.into_inner();
        self.to_bft_tx.send(BftSvrMsg::PProof(pp)).unwrap();
        let reply = SimpleResponse { is_success: true };
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

    // Connect to the controller. Retry on failure.
    async fn run(mut b2c: BftToCtl) {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        let controller_addr = format!("http://127.0.0.1:{}", b2c.ctr_port);
        info!("connecting to controller...");

        let client = loop {
            interval.tick().await;
            match ControllerClient::connect(controller_addr.clone()).await {
                Ok(client) => break client,
                Err(e) => {
                    debug!("connect to controller failed: `{}`", e);
                }
            }
            debug!("Retrying to connect controller");
        };

        while let Some(to_msg) = b2c.to_ctl_rx.recv().await {}
    }
}

struct BftToNet {
    net_port: u16,
    self_port: String,
    to_net_rx: mpsc::UnboundedReceiver<BftToNetMsg>,
}

impl BftToNet {
    pub fn new(
        net_port: u16,
        self_port: String,
        to_net_rx: mpsc::UnboundedReceiver<BftToNetMsg>,
    ) -> Self {
        BftToNet {
            net_port,
            self_port,
            to_net_rx,
        }
    }

    async fn run(mut b2n: BftToNet) {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        let net_addr = format!("http://127.0.0.1:{}", b2n.net_port);
        info!("connecting to network...");
        let mut client = loop {
            interval.tick().await;
            match NetworkClient::connect(net_addr.clone()).await {
                Ok(client) => break client,
                Err(e) => {
                    debug!("connect to network failed: `{}`", e);
                }
            }
            debug!("Retrying to connect network");
        };

        let request = Request::new(RegisterInfo {
            module_name: "consensus".to_owned(),
            hostname: "127.0.0.1".to_owned(),
            port: b2n.self_port,
        });
        // tobe fix
        let response = client.register_network_msg_handler(request).await.unwrap();
        if response.into_inner().is_success {}
        while let Some(msg) = b2n.to_net_rx.recv().await {}
    }
    // Connect to the network. Retry on failure.

    // pub async fn register_network_msg_handler(&self) -> Result<bool, Box<dyn std::error::Error>> {
    //     let network_addr = format!("http://127.0.0.1:{}", self.net_port);
    //     let mut client = connect_network(network_addr);
    // }
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
            self.to_bft_tx.send(msg).unwrap();
            let reply = SimpleResponse { is_success: true };
            Ok(tonic::Response::new(reply))
        }
    }
}

#[tokio::main]
async fn run() {
    ::std::env::set_var("RUST_BACKTRACE", "full");

    let (bft_tx, bft_rx) = mpsc::unbounded_channel();
    let bft_svr = BftSvr::new(bft_tx);

    let (to_ctl_tx, to_ctl_rx) = mpsc::unbounded_channel();
    let (ctl_back_tx, ctl_back_rx) = mpsc::unbounded_channel();
    let b2c = BftToCtl::new(8001, to_ctl_rx, ctl_back_tx);

    tokio::spawn(async move {
        BftToCtl::run(b2c).await;
    });

    let (to_net_tx, to_net_rx) = mpsc::unbounded_channel();
    let b2n = BftToNet::new(8003, 8004.to_string(), to_net_rx);
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

    let sk = PrivateKey::new("0xaa");
    let bft_params = BftParams::new(&sk);
    let mut bft = Bft::new(bft_params, bc);

    tokio::spawn(async move {
        bft.start().await;
    });

    let addr_str = format!("127.0.0.1:{}", 8000);
    let addr = addr_str.parse().unwrap();
    Server::builder()
        .add_service(ConsensusServiceServer::new(bft_svr))
        .add_service(NetworkMsgHandlerServiceServer::new(n2b))
        .serve(addr)
        .await;
}

fn main() {
    run();
}
