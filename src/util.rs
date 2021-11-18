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

use crate::config::BftConfig;
use cita_cloud_proto::kms::kms_service_client::KmsServiceClient;
use cita_types::H256;
use cloud_util::crypto::{hash_data, recover_signature, sign_message};
use tokio::sync::OnceCell;
use tonic::transport::{Channel, Endpoint};

pub static KMS_CLIENT: OnceCell<KmsServiceClient<Channel>> = OnceCell::const_new();

// This must be called before access to clients.
pub fn init_grpc_client(config: &BftConfig) {
    KMS_CLIENT
        .set({
            let addr = format!("http://127.0.0.1:{}", config.kms_port);
            let channel = Endpoint::from_shared(addr).unwrap().connect_lazy().unwrap();
            KmsServiceClient::new(channel)
        })
        .unwrap();
}

pub fn kms_client() -> KmsServiceClient<Channel> {
    KMS_CLIENT.get().cloned().unwrap()
}

pub fn hash_msg(msg: &[u8]) -> H256 {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move { H256::from_slice(&hash_data(kms_client(), msg).await.unwrap()) })
}

pub fn sign_msg(msg: &[u8], key_id: u64) -> Vec<u8> {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        let hash = hash_data(kms_client(), msg).await.unwrap();
        sign_message(kms_client(), key_id, &hash).await.unwrap()
    })
}

pub fn recover_sig(sig: &[u8], msg: &[u8]) -> Vec<u8> {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        let hash = hash_data(kms_client(), msg).await.unwrap();
        recover_signature(kms_client(), sig, &hash).await.unwrap()
    })
}
