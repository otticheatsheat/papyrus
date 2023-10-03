use std::collections::HashMap;
use std::time::Duration;

use defaultmap::DefaultHashMap;
use futures::StreamExt;
use libp2p::swarm::dial_opts::DialOpts;
use libp2p::swarm::{ConnectionHandler, NetworkBehaviour, SwarmEvent};
use libp2p::{PeerId, Swarm};

use super::behaviour::{Behaviour, Event};
use super::OutboundSessionId;
use crate::messages::block::{BlockHeader, GetBlocks, GetBlocksResponse};
use crate::messages::common::BlockId;
use crate::messages::proto::p2p::proto::get_blocks_response::Response;
use crate::test_utils::{create_swarm, StreamHashMap};

const NUM_PEERS: usize = 5;

async fn collect_events_from_swarms<BehaviourTrait: NetworkBehaviour, T>(
    swarms: &mut StreamHashMap<PeerId, Swarm<BehaviourTrait>>,
    mut map_and_filter_event: impl FnMut(
        PeerId,
        SwarmEvent<
            <BehaviourTrait as NetworkBehaviour>::ToSwarm,
            <<BehaviourTrait as NetworkBehaviour>::ConnectionHandler as ConnectionHandler>::Error,
        >,
    ) -> Option<(PeerId, T)>,
    assert_unique: bool,
) -> HashMap<(PeerId, PeerId), T> {
    let mut results = HashMap::<(PeerId, PeerId), T>::new();
    loop {
        // Swarms should never finish, so we can unwrap the option.
        let (peer_id, event) = swarms.next().await.unwrap();
        if let Some((other_peer_id, value)) = map_and_filter_event(peer_id, event) {
            let is_unique = results.insert((peer_id, other_peer_id), value).is_none();
            if assert_unique {
                assert!(is_unique);
            }
            if results.len() == (NUM_PEERS - 1) * NUM_PEERS {
                break;
            }
        }
    }
    results
}

#[tokio::test]
async fn everyone_sends_to_everyone() {
    const NUM_MESSAGES_PER_SESSION: usize = 4;
    let substream_timeout = Duration::from_secs(3600);

    let mut swarms_and_addresses = (0..NUM_PEERS)
        .map(|_| create_swarm(Behaviour::<GetBlocks, GetBlocksResponse>::new(substream_timeout)))
        .collect::<Vec<_>>();

    let mut peer_id_to_index = HashMap::<PeerId, usize>::new();
    // TODO(shahak): Change comments to functions
    // dial to other swarms.
    for i in 0..NUM_PEERS {
        peer_id_to_index.insert(*swarms_and_addresses[i].0.local_peer_id(), i);
        for j in 0..NUM_PEERS {
            if i == j {
                continue;
            }
            let (inbound_peer_id, inbound_address) = {
                let (inbound_swarm, inbound_address) = &swarms_and_addresses[j];
                (*inbound_swarm.local_peer_id(), inbound_address.clone())
            };
            let (outbound_swarm, _) = swarms_and_addresses.get_mut(i).unwrap();
            outbound_swarm
                .dial(DialOpts::peer_id(inbound_peer_id).addresses(vec![inbound_address]).build())
                .unwrap();
        }
    }

    // Collect swarms to a single stream
    let mut swarms = StreamHashMap::new(
        swarms_and_addresses
            .into_iter()
            .map(|(swarm, _)| (*swarm.local_peer_id(), swarm))
            .collect(),
    );

    // wait until all connections are established
    collect_events_from_swarms(
        &mut swarms,
        |_peer_id, swarm_event| {
            let SwarmEvent::ConnectionEstablished { peer_id: other_peer_id, .. } = swarm_event
            else {
                return None;
            };
            Some((other_peer_id, ()))
        },
        false,
    )
    .await;

    // Call send_query for each pair of swarms.
    let mut outbound_session_id_to_peer_id = HashMap::<(PeerId, OutboundSessionId), PeerId>::new();
    for outbound_swarm in swarms.values_mut() {
        let outbound_peer_id = *outbound_swarm.local_peer_id();
        let outbound_index = peer_id_to_index[&outbound_peer_id];
        for (inbound_peer_id, inbound_index) in peer_id_to_index.clone() {
            if inbound_peer_id == outbound_peer_id {
                continue;
            }
            let outbound_session_id = outbound_swarm
                .behaviour_mut()
                .send_query(
                    GetBlocks {
                        step: (outbound_index * NUM_PEERS + inbound_index) as u64,
                        ..Default::default()
                    },
                    inbound_peer_id,
                )
                .unwrap();
            outbound_session_id_to_peer_id
                .insert((outbound_peer_id, outbound_session_id), inbound_peer_id);
        }
    }

    // collect all NewInboundSession events.
    let inbound_session_ids = collect_events_from_swarms(
        &mut swarms,
        |peer_id, swarm_event| {
            let SwarmEvent::Behaviour(event) = swarm_event else {
                return None;
            };
            let Event::NewInboundSession { query, inbound_session_id, peer_id: other_peer_id } =
                event
            else {
                panic!("Got unexpected event {:?} when expecting NewInboundSession", event);
            };
            assert_eq!(
                query.step as usize,
                peer_id_to_index[&other_peer_id] * NUM_PEERS + peer_id_to_index[&peer_id]
            );
            Some((other_peer_id, inbound_session_id))
        },
        true,
    )
    .await;

    // Call send_data for each pair of swarms.
    for inbound_swarm in swarms.values_mut() {
        let inbound_peer_id = *inbound_swarm.local_peer_id();
        let inbound_index = peer_id_to_index[&inbound_peer_id];
        for (outbound_peer_id, outbound_index) in peer_id_to_index.clone() {
            if inbound_peer_id == outbound_peer_id {
                continue;
            }
            for i in 0..NUM_MESSAGES_PER_SESSION {
                inbound_swarm
                    .behaviour_mut()
                    .send_data(
                        GetBlocksResponse {
                            response: Some(Response::Header(BlockHeader {
                                parent_block: Some(BlockId {
                                    hash: None,
                                    height: ((outbound_index * NUM_PEERS + inbound_index)
                                        * NUM_MESSAGES_PER_SESSION
                                        + i) as u64,
                                }),
                                ..Default::default()
                            })),
                        },
                        inbound_session_ids[&(inbound_peer_id, outbound_peer_id)],
                    )
                    .unwrap();
            }
        }
    }

    // collect all ReceivedData events.
    let mut current_message = DefaultHashMap::<(PeerId, PeerId), usize>::new();
    collect_events_from_swarms(
        &mut swarms,
        move |peer_id, swarm_event| {
            let SwarmEvent::Behaviour(event) = swarm_event else {
                return None;
            };
            let Event::ReceivedData { outbound_session_id, data } = event else {
                panic!("Got unexpected event {:?} when expecting ReceivedData", event);
            };
            let other_peer_id = outbound_session_id_to_peer_id[&(peer_id, outbound_session_id)];
            let GetBlocksResponse {
                response:
                    Some(Response::Header(BlockHeader {
                        parent_block: Some(BlockId { height, .. }),
                        ..
                    })),
            } = data
            else {
                panic!("Got unexpected data {:?}", data);
            };
            let message_index = current_message.get((peer_id, other_peer_id));
            assert_eq!(
                height as usize,
                (peer_id_to_index[&peer_id] * NUM_PEERS + peer_id_to_index[&other_peer_id])
                    * NUM_MESSAGES_PER_SESSION
                    + message_index,
            );
            current_message.insert((peer_id, other_peer_id), message_index + 1);
            Some((other_peer_id, ()))
        },
        false,
    )
    .await;
}
