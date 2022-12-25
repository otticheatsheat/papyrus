use std::sync::Arc;

use async_stream::stream;
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::{future, pin_mut, TryStreamExt};
use futures_util::StreamExt;
use indexmap::IndexMap;
use log::{debug, error, info};
#[cfg(test)]
use mockall::automock;
use serde::{Deserialize, Serialize};
use starknet_api::block::{Block, BlockHash, BlockNumber};
use starknet_api::core::ClassHash;
use starknet_api::state::{ContractClass, StateDiff};
use starknet_api::StarknetApiError;
use starknet_client::{
    ClientCreationError, ClientError, RetryConfig, StarknetClient, StarknetClientTrait, StateUpdate,
};
use tokio_stream::Stream;

use super::stream_utils::MyStreamExt;

pub type CentralResult<T> = Result<T, CentralError>;
#[derive(Clone, Serialize, Deserialize)]
pub struct CentralSourceConfig {
    pub concurrent_requests: usize,
    pub url: String,
    pub retry_config: RetryConfig,
}
pub struct GenericCentralSource<TStarknetClient: StarknetClientTrait + Send + Sync> {
    pub concurrent_requests: usize,
    pub starknet_client: Arc<TStarknetClient>,
}

#[derive(thiserror::Error, Debug)]
pub enum CentralError {
    #[error(transparent)]
    ClientCreation(#[from] ClientCreationError),
    #[error(transparent)]
    ClientError(#[from] Arc<ClientError>),
    #[error("Could not find a state update.")]
    StateUpdateNotFound,
    #[error("Could not find a block with block number {:?}.", block_number)]
    BlockNotFound { block_number: BlockNumber },
    #[error(transparent)]
    StarknetApiError(#[from] Arc<StarknetApiError>),
}

#[cfg_attr(test, automock)]
#[async_trait]
pub trait CentralSourceTrait {
    async fn get_block_marker(&self) -> Result<BlockNumber, ClientError>;
    fn stream_new_blocks(
        &self,
        initial_block_number: BlockNumber,
        up_to_block_number: BlockNumber,
    ) -> BlocksStream<'_>;
    fn stream_state_updates(
        &self,
        initial_block_number: BlockNumber,
        up_to_block_number: BlockNumber,
    ) -> StateUpdatesStream<'_>;

    async fn get_block_hash(
        &self,
        block_number: BlockNumber,
    ) -> Result<Option<BlockHash>, ClientError>;
}

pub(crate) type BlocksStream<'a> = BoxStream<'a, Result<(BlockNumber, Block), CentralError>>;
pub(crate) type StateUpdatesStream<'a> = BoxStream<
    'a,
    CentralResult<(BlockNumber, BlockHash, StateDiff, Vec<(ClassHash, ContractClass)>)>,
>;

#[async_trait]
impl<TStarknetClient: StarknetClientTrait + Send + Sync + 'static> CentralSourceTrait
    for GenericCentralSource<TStarknetClient>
{
    async fn get_block_marker(&self) -> Result<BlockNumber, ClientError> {
        self.starknet_client
            .block_number()
            .await?
            .map_or(Ok(BlockNumber::default()), |block_number| Ok(block_number.next()))
    }

    async fn get_block_hash(
        &self,
        block_number: BlockNumber,
    ) -> Result<Option<BlockHash>, ClientError> {
        self.starknet_client
            .block(block_number)
            .await?
            .map_or(Ok(None), |block| Ok(Some(block.block_hash)))
    }

    fn stream_state_updates(
        &self,
        initial_block_number: BlockNumber,
        up_to_block_number: BlockNumber,
    ) -> StateUpdatesStream<'_> {
        let mut current_block_number = initial_block_number;
        let stream = stream! {
            while current_block_number < up_to_block_number {
                let state_update_stream = self.state_update_stream(futures_util::stream::iter(
                    current_block_number.iter_up_to(up_to_block_number),
                ));
                pin_mut!(state_update_stream);
                while let Some(maybe_state_update) = state_update_stream.next().await {
                    match maybe_state_update {
                        Ok((state_update, classes)) => {
                            let block_hash = state_update.block_hash;
                            let (declared_classes, deployed_contract_class_definitions) =
                                classes.split_at(state_update.state_diff.declared_contracts.len());
                            let state_diff = StateDiff {
                                deployed_contracts: IndexMap::from_iter(
                                    state_update
                                        .state_diff
                                        .deployed_contracts
                                        .iter()
                                        .map(|dc| (dc.address, dc.class_hash)),
                                ),
                                storage_diffs: IndexMap::from_iter(
                                    state_update.state_diff.storage_diffs.into_iter().map(
                                        |(address, entries)| {
                                            (
                                                address,
                                                entries
                                                    .into_iter()
                                                    .map(|se| (se.key, se.value))
                                                    .collect(),
                                            )
                                        },
                                    ),
                                ),
                                declared_classes: IndexMap::from_iter(
                                    declared_classes.to_vec().into_iter(),
                                ),
                                nonces: state_update.state_diff.nonces,
                            };
                            yield Ok((
                                current_block_number,
                                block_hash,
                                state_diff,
                                deployed_contract_class_definitions.to_vec(),
                            ));
                            current_block_number = current_block_number.next();
                        }
                        Err(err) => {
                            debug!("Block number {}: {:#?}", current_block_number, err);
                            yield Err(err);
                            return;
                        }
                    }
                }
            }
        };
        stream.boxed()
    }

    // TODO(shahak): rename.
    fn stream_new_blocks(
        &self,
        initial_block_number: BlockNumber,
        up_to_block_number: BlockNumber,
    ) -> BlocksStream<'_> {
        let mut current_block_number = initial_block_number;
        let stream = stream! {
            while current_block_number < up_to_block_number {
                let mut res =
                    futures_util::stream::iter(current_block_number.iter_up_to(up_to_block_number))
                        .map(|bn| async move { self.starknet_client.block(bn).await })
                        .buffered(self.concurrent_requests);
                while let Some(maybe_block) = res.next().await {
                    let res = match maybe_block {
                        Ok(Some(block)) => {
                            info!("Received new block: {}.", block.block_number);
                            Block::try_from(block)
                                .map_err(|err| CentralError::ClientError(Arc::new(err)))
                        }
                        Ok(None) => {
                            Err(CentralError::BlockNotFound { block_number: current_block_number })
                        }
                        Err(err) => Err(CentralError::ClientError(Arc::new(err))),
                    };
                    match res {
                        Ok(block) => {
                            yield Ok((current_block_number, block));
                            current_block_number = current_block_number.next();
                        }
                        Err(err) => {
                            debug!(
                                "Received error for block {}: {:?}.",
                                current_block_number, err
                            );
                            yield (Err(err));
                            return;
                        }
                    }
                }
            }
        };
        stream.boxed()
    }
}

impl<TStarknetClient: StarknetClientTrait + Send + Sync + 'static>
    GenericCentralSource<TStarknetClient>
{
    fn state_update_stream(
        &self,
        block_number_stream: impl Stream<Item = BlockNumber> + Send + Sync + 'static,
    ) -> impl Stream<Item = CentralResult<(StateUpdate, Vec<(ClassHash, ContractClass)>)>> {
        // Stream the state updates.
        let starknet_client = self.starknet_client.clone();
        let (state_updates0, mut state_updates1) = block_number_stream
            .map(move |block_number| {
                let starknet_client = starknet_client.clone();
                async move { starknet_client.state_update(block_number).await }
            })
            .buffered(self.concurrent_requests)
            // Client error is not cloneable.
            .map_err(Arc::new)
            .fanout(self.concurrent_requests);

        // Stream the declared and deployed classes.
        let starknet_client = self.starknet_client.clone();
        let mut flat_classes = state_updates0
            // In case state_updates1 contains a ClientError, we yield it and break - without
            // evaluating flat_classes.
            .filter_map(|state_update| future::ready(state_update.ok()))
            .filter_map(future::ready)
            .map(|state_update| state_update.state_diff.class_hashes())
            .flat_map(futures::stream::iter)
            .map(move |class_hash| {
                let starknet_client = starknet_client.clone();
                async move { (class_hash, starknet_client.class_by_hash(class_hash).await) }
            })
            .buffered(self.concurrent_requests)
            .map(|(class_hash, class)| (class_hash, class.map_err(Arc::new)));

        let res_stream = stream! {
            while let Some(maybe_state_update) = state_updates1.next().await {
                // Get the next state update.
                let state_update = match maybe_state_update {
                    Ok(Some(state_update)) => state_update,
                    Ok(None) => {
                        yield (Err(CentralError::StateUpdateNotFound));
                        break;
                    }
                    Err(err) => {
                        match err {
                            _ => yield (Err(CentralError::ClientError(err))),
                        }
                        break;
                    }
                };

                // Get the next state declared and deployed classes.
                let len = state_update.state_diff.class_hashes().len();
                let classes: Result<Vec<(ClassHash, ContractClass)>, _> = flat_classes
                    .take_n(len)
                    .await
                    .expect("Failed to download state update")
                    .into_iter()
                    .map(|(class_hash, class)| {
                        match class{
                            Ok(Some(class)) => Ok((class_hash, class.into())),
                            Ok(None) => Err(CentralError::StateUpdateNotFound),
                            Err(err) => Err(CentralError::ClientError(err)),
                        }
                    })
                    .collect();
                match classes {
                    Ok(classes) => yield (Ok((state_update, classes))),
                    Err(err) => yield (Err(err)),
                }
            }
        };
        res_stream
    }
}

pub type CentralSource = GenericCentralSource<StarknetClient>;

impl CentralSource {
    pub fn new(config: CentralSourceConfig) -> Result<CentralSource, ClientCreationError> {
        let starknet_client = StarknetClient::new(&config.url, config.retry_config)?;
        info!("Central source is configured with {}.", config.url);
        Ok(CentralSource {
            concurrent_requests: config.concurrent_requests,
            starknet_client: Arc::new(starknet_client),
        })
    }
}
