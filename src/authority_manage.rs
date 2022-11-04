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

use crate::params::BftParams;
use crate::types::Address;
use bincode::{deserialize, serialize};
use std::fs::{read_dir, DirBuilder, File, OpenOptions};
use std::io;
use std::io::{Read, Seek, Write};
use std::mem::transmute;
use std::path::Path;
use std::str;

#[derive(Debug)]
pub(crate) struct Wal {
    fs: File,
}

impl Wal {
    pub(crate) fn create(dir: &str) -> Result<Wal, io::Error> {
        let fss = read_dir(&dir);
        if fss.is_err() {
            DirBuilder::new().recursive(true).create(dir).unwrap();
        }

        let fpath = dir.to_string() + "/authorities_old";
        let big_path = Path::new(&*fpath).to_path_buf();
        let fs = OpenOptions::new()
            .read(true)
            .create(true)
            .write(true)
            .open(big_path)?;
        Ok(Wal { fs })
    }

    pub(crate) fn save(&mut self, mtype: u8, msg: &[u8]) -> io::Result<usize> {
        let mlen = msg.len() as u32;
        if mlen == 0 {
            return Ok(0);
        }
        self.fs.set_len(0)?;
        let len_bytes: [u8; 4] = mlen.to_le_bytes();
        let type_bytes: [u8; 1] = mtype.to_le_bytes();
        self.fs.seek(io::SeekFrom::End(0))?;
        self.fs.write_all(&len_bytes[..])?;
        self.fs.write_all(&type_bytes[..])?;
        let hlen = self.fs.write(msg)?;
        self.fs.flush()?;
        Ok(hlen)
    }

    pub(crate) fn load(&mut self) -> Vec<(u8, Vec<u8>)> {
        let mut vec_buf: Vec<u8> = Vec::new();
        let mut vec_out: Vec<(u8, Vec<u8>)> = Vec::new();

        self.fs.seek(io::SeekFrom::Start(0)).unwrap();
        let res_fsize = self.fs.read_to_end(&mut vec_buf);
        if res_fsize.is_err() {
            return vec_out;
        }
        let fsize = res_fsize.unwrap();
        if fsize <= 5 {
            return vec_out;
        }
        let mut index = 0;
        loop {
            if index + 5 > fsize {
                break;
            }
            let hd: [u8; 4] = [
                vec_buf[index],
                vec_buf[index + 1],
                vec_buf[index + 2],
                vec_buf[index + 3],
            ];
            let tmp: u32 = unsafe { transmute::<[u8; 4], u32>(hd) };
            let bodylen = tmp as usize;
            let mtype = vec_buf[index + 4];
            index += 5;
            if index + bodylen > fsize {
                break;
            }
            vec_out.push((mtype, vec_buf[index..index + bodylen].to_vec()));
            index += bodylen;
        }
        vec_out
    }
}

const LOG_TYPE_AUTHORITIES: u8 = 1;

#[derive(Debug)]
pub struct AuthorityManage {
    pub authorities: Vec<Address>,
    pub validators: Vec<Address>,
    authorities_log: Wal,
    pub authorities_old: Vec<Address>,
    pub validators_old: Vec<Address>,
    pub authority_h_old: usize,
}

impl AuthorityManage {
    pub fn new(params: &BftParams) -> Self {
        let logpath = params.authority_path.clone();

        let mut authority_manage = AuthorityManage {
            authorities: Vec::new(),
            validators: Vec::new(),
            authorities_log: Wal::create(&*logpath).unwrap(),
            authorities_old: Vec::new(),
            validators_old: Vec::new(),
            authority_h_old: 0,
        };

        let vec_out = authority_manage.authorities_log.load();
        if !vec_out.is_empty() {
            if let Ok((h, authorities, validators_old, validators)) = deserialize(&(vec_out[0].1)) {
                let authorities: Vec<Address> = authorities;
                let validators_old: Vec<Address> = validators_old;
                let validators: Vec<Address> = validators;

                authority_manage.authorities.extend_from_slice(&authorities);
                authority_manage
                    .validators_old
                    .extend_from_slice(&validators_old);
                authority_manage.authority_h_old = h;
                authority_manage.validators.extend_from_slice(&validators);
            }
        }

        authority_manage
    }

    pub fn validator_n(&self) -> usize {
        self.validators.len()
    }

    pub fn receive_authorities_list(
        &mut self,
        height: usize,
        authorities: &[Address],
        validators: &[Address],
    ) {
        let flag = if self.validators != validators {
            2
        } else if self.authorities != authorities {
            1
        } else {
            0
        };

        if flag > 0 {
            self.authorities_old.clear();
            self.authorities_old.extend_from_slice(&self.authorities);
            self.validators_old.clear();
            self.validators_old.extend_from_slice(&self.validators);
            self.authority_h_old = height;

            self.authorities.clear();
            self.authorities.extend_from_slice(authorities);
            self.validators.clear();
            self.validators.extend_from_slice(validators);

            if flag == 2 {
                self.save();
            }
        }
    }

    pub fn save(&mut self) {
        let bmsg = serialize(&(
            self.authority_h_old,
            self.authorities.clone(),
            self.validators_old.clone(),
            self.validators.clone(),
        ))
        .unwrap();
        let _ = self.authorities_log.save(LOG_TYPE_AUTHORITIES, &bmsg);
    }
}
