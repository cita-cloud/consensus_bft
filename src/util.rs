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
use cita_cloud_proto::client::{ClientOptions, InterceptedSvc};
use cita_cloud_proto::crypto::crypto_service_client::CryptoServiceClient;
use cita_cloud_proto::retry::RetryClient;
use cita_types::H256;
use cloud_util::crypto::{hash_data, recover_signature, sign_message};
use tokio::sync::OnceCell;

pub static CRYPTO_CLIENT: OnceCell<RetryClient<CryptoServiceClient<InterceptedSvc>>> =
    OnceCell::const_new();

// This must be called before access to clients.
pub fn init_grpc_client(config: &BftConfig) {
    CRYPTO_CLIENT
        .set({
            let client_options = ClientOptions::new(
                "crypto".to_string(),
                format!("http://127.0.0.1:{}", config.crypto_port),
            );
            match client_options.connect_crypto() {
                Ok(retry_client) => retry_client,
                Err(e) => panic!("client init error: {:?}", &e),
            }
        })
        .unwrap();
}

pub fn crypto_client() -> RetryClient<CryptoServiceClient<InterceptedSvc>> {
    CRYPTO_CLIENT.get().cloned().unwrap()
}

pub fn hash_msg(msg: &[u8]) -> H256 {
    tokio::task::block_in_place(move || {
        tokio::runtime::Handle::current().block_on(async move {
            H256::from_slice(&hash_data(crypto_client(), msg).await.unwrap())
        })
    })
}

pub fn sign_msg(msg: &[u8]) -> Vec<u8> {
    tokio::task::block_in_place(move || {
        tokio::runtime::Handle::current().block_on(async move {
            let hash = hash_data(crypto_client(), msg).await.unwrap();
            sign_message(crypto_client(), &hash).await.unwrap()
        })
    })
}

pub fn recover_sig(sig: &[u8], msg: &[u8]) -> Vec<u8> {
    tokio::task::block_in_place(move || {
        tokio::runtime::Handle::current().block_on(async move {
            let hash = hash_data(crypto_client(), msg).await.unwrap();
            recover_signature(crypto_client(), sig, &hash)
                .await
                .unwrap()
        })
    })
}
