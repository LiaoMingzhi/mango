// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

use crate::block::Block;
use async_trait::async_trait;

/// The interfaces to verify the legitimacy of a statement block's contents.
#[async_trait]
pub trait BlockVerifier: Send + Sync + 'static {
    type Error: std::fmt::Display + std::fmt::Debug + Send + Sync + 'static;
    /// Determines if a statement block's content is valid.
    #[allow(dead_code)]
    async fn verify(&self, _b: &Block) -> Result<(), Self::Error>;

    #[allow(dead_code)]
    async fn verify_all(&self, _b: &[Block]) -> Result<(), Self::Error>;
}

#[derive(Clone)]
#[allow(dead_code)]
pub struct TestBlockVerifier;

#[async_trait]
impl BlockVerifier for TestBlockVerifier {
    type Error = String;

    async fn verify(&self, _b: &Block) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn verify_all(&self, _b: &[Block]) -> Result<(), Self::Error> {
        Ok(())
    }
}
