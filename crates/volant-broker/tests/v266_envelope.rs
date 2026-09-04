//! v0.266: Kafka Envelope key 58 v0 reject.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use bytes::{Buf, BytesMut};
use common::{boot_kafka, broker_temp, rpc};
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_bytes, put_compact_bytes,
    put_empty_tag_buffer, skip_tag_buffer,
};
use volant_broker::Broker;

fn envelope_v0(
    request_data: Option<&[u8]>,
    principal: Option<&[u8]>,
    client_host: Option<&[u8]>,
) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_bytes(&mut body, request_data);
    put_compact_bytes(&mut body, principal);
    put_compact_bytes(&mut body, client_host);
    put_empty_tag_buffer(&mut body);
    body
}

fn overlay_ids(b: &Broker) -> Vec<u32> {
    b.list_membership().brokers.iter().map(|x| x.id).collect()
}

fn topic_names(b: &Broker) -> Vec<String> {
    let mut names: Vec<String> = b
        .metadata(None)
        .topics
        .into_iter()
        .map(|t| t.name.as_str().to_owned())
        .collect();
    names.sort();
    names
}

#[tokio::test]
async fn api_versions_lists_envelope_58() {
    let (_dir, broker) = broker_temp("v266", "api");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let resp = rpc(&addr, encode_request(18, 0, 1, Some("t"), &[])).await;
    let mut src = resp.freeze();
    src.advance(4 + 2);
    let n = src.get_i32();
    let mut found = std::collections::HashMap::new();
    for _ in 0..n {
        let key = src.get_i16();
        let min_v = src.get_i16();
        let max_v = src.get_i16();
        found.insert(key, (min_v, max_v));
    }
    assert!(found.len() >= 65);
    assert_eq!(found.get(&58), Some(&(0, 0)));
    assert_eq!(found.get(&57), Some(&(0, 1)));
    assert_eq!(found.get(&60), Some(&(0, 2)));

    server.abort();
}

#[tokio::test]
async fn envelope_v0_is_42_response_data_null() {
    let (_dir, broker) = broker_temp("v266", "env");
    broker.create_topic("events", 1).unwrap();
    let before_topics = topic_names(&broker);
    let before_ids = overlay_ids(&broker);

    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(
        &addr,
        encode_request_flexible(
            58,
            0,
            10,
            Some("admin"),
            &envelope_v0(Some(b"create-topics-dummy"), None, Some(b"127.0.0.1")),
        ),
    )
    .await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 10);
    skip_tag_buffer(&mut src).unwrap(); // header v1 (always flex)
    let data = get_compact_bytes(&mut src).unwrap();
    assert!(data.is_none(), "ResponseData must be null (uvarint 0)");
    assert_eq!(src.get_i16(), 42); // INVALID_REQUEST
    skip_tag_buffer(&mut src).unwrap();
    assert_eq!(src.remaining(), 0);

    assert_eq!(topic_names(&broker), before_topics);
    assert_eq!(overlay_ids(&broker), before_ids);
    server.abort();
}

#[tokio::test]
async fn envelope_v1_unsupported() {
    let (_dir, broker) = broker_temp("v266", "v1");
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;
    let resp = rpc(&addr, encode_request_flexible(58, 1, 99, Some("c"), &[])).await;
    let mut src = resp.freeze();
    assert_eq!(src.get_i32(), 99);
    skip_tag_buffer(&mut src).unwrap(); // header v1 (always flex)
    assert_eq!(src.get_i16(), 35); // UnsupportedVersion

    server.abort();
}
