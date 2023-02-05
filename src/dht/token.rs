use std::net::{SocketAddr};

use crate::addr::SocketAddrBytes;

const SECRET_SIZE: usize = 8;
const TOKEN_SIZE: usize = 8;

#[derive(Debug, Default)]
pub struct TokenManager {
    secrets: Vec<[u8; SECRET_SIZE]>,
}

impl TokenManager {
    pub fn new() -> Self {
        let mut tm = Self {
            secrets: vec![[0u8; SECRET_SIZE]; 2],
        };
        tm.refresh_secret();
        tm
    }

    pub fn make_token(&self, addr: &SocketAddr) -> Vec<u8> {
        Self::make_token_inner(&self.secrets[0], addr)
    }

    fn make_token_inner(secret: &[u8; SECRET_SIZE], addr: &SocketAddr) -> Vec<u8> {
        let mut input = vec![];
        input.extend(secret);
        input.extend(SocketAddrBytes::from(*addr).to_bytes());
        let d = ring::digest::digest(&ring::digest::SHA256, &input);
        d.as_ref()[..TOKEN_SIZE].to_vec()
    }

    pub fn verify(&self, addr: &SocketAddr, token: &[u8]) -> bool {
        if token.len() != TOKEN_SIZE {
            return false;
        }
        for secret in &self.secrets {
            if Self::make_token_inner(secret, addr) == token {
                return true;
            }
        }
        false
    }

    pub fn refresh_secret(&mut self) {
        self.secrets[1] = self.secrets[0];
        self.secrets[0] = rand::random();
    }
}
