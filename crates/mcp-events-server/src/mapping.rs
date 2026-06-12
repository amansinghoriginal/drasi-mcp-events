//! Event modeling: builds the event-type registry + per-type param filters
//! from the configured queries, maps `FeedEvent`s to `EmittedEvent`s, and
//! wires the feed source into the buffer.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use drasi_feed::{ChangeType, FeedEvent};
use mcp_events_engine::{EmittedEvent, ParamFilter};
use mcp_events_wire::{DeliveryMode, EventDefinition, JsonRpcError};
use serde_json::{json, Map, Value};

use crate::config::{EventModeling, FeedKind, ServerConfig};
use crate::state::AppState;

pub const CHANGE_TYPES: [&str; 3] = ["added", "updated", "deleted"];

fn change_type_str(c: ChangeType) -> &'static str {
    match c {
        ChangeType::Added => "added",
        ChangeType::Updated => "updated",
        ChangeType::Deleted => "deleted",
    }
}

fn change_type_input_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "changeType": {
                "type": "string",
                "enum": CHANGE_TYPES,
                "description": "Only deliver result-set changes of this type"
            }
        }
    })
}

/// Builds the `events/list` definitions and the param filters enforced by
/// poll/stream/webhook reads, per the configured `eventModeling` mode.
pub fn build_event_model(
    config: &ServerConfig,
) -> (Vec<EventDefinition>, HashMap<String, Arc<ParamFilter>>) {
    let mut delivery = vec![DeliveryMode::Poll, DeliveryMode::Push];
    if config.webhook.enabled {
        delivery.push(DeliveryMode::Webhook);
    }
    let mut defs = Vec::new();
    let mut filters: HashMap<String, Arc<ParamFilter>> = HashMap::new();
    for q in &config.queries {
        // queries[].payloadSchema describes the projected row; event payload
        // schemas are derived from it (assumption recorded in spec gaps).
        let row = q
            .payload_schema
            .clone()
            .unwrap_or_else(|| json!({ "type": "object" }));
        match config.event_modeling {
            EventModeling::Single => {
                let name = format!("{}.changed", q.id);
                defs.push(EventDefinition {
                    name: name.clone(),
                    description: Some(q.description.clone().unwrap_or_else(|| {
                        format!(
                            "Rows entering/leaving/changing in the `{}` continuous query",
                            q.id
                        )
                    })),
                    delivery: delivery.clone(),
                    input_schema: Some(change_type_input_schema()),
                    payload_schema: Some(json!({
                        "type": "object",
                        "properties": {
                            "changeType": { "type": "string", "enum": CHANGE_TYPES },
                            "before": row,
                            "after": row,
                        },
                        "required": ["changeType"]
                    })),
                    meta: None,
                });
                let filter: Arc<ParamFilter> = Arc::new(|params: &Value, data: &Value| {
                    match params.get("changeType") {
                        None | Some(Value::Null) => true,
                        Some(Value::String(want)) => {
                            data.get("changeType").and_then(Value::as_str) == Some(want.as_str())
                        }
                        Some(_) => false,
                    }
                });
                filters.insert(name, filter);
            }
            EventModeling::PerChange => {
                let variants: [(&str, String, Value); 3] = [
                    (
                        "added",
                        format!("Row entered the `{}` result set", q.id),
                        row.clone(),
                    ),
                    (
                        "updated",
                        format!("Row in the `{}` result set changed", q.id),
                        json!({
                            "type": "object",
                            "properties": { "before": row, "after": row },
                            "required": ["after"]
                        }),
                    ),
                    (
                        "deleted",
                        format!("Row left the `{}` result set", q.id),
                        row.clone(),
                    ),
                ];
                for (suffix, desc, payload) in variants {
                    defs.push(EventDefinition {
                        name: format!("{}.{suffix}", q.id),
                        description: Some(match &q.description {
                            Some(d) => format!("{d} ({suffix})"),
                            None => desc,
                        }),
                        delivery: delivery.clone(),
                        input_schema: None,
                        payload_schema: Some(payload),
                        meta: None,
                    });
                }
            }
        }
    }
    (defs, filters)
}

/// Maps one feed change to an emitted event per the modeling mode. Returns
/// `None` (with a log) for changes missing the side their mode requires.
pub fn map_feed_event(mode: EventModeling, ev: FeedEvent) -> Option<EmittedEvent> {
    let event_id = ev
        .upstream_id
        .clone()
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let timestamp = ev.timestamp.unwrap_or_else(Utc::now);
    let (name, data) = match mode {
        EventModeling::Single => {
            let mut data = Map::new();
            data.insert(
                "changeType".to_owned(),
                Value::from(change_type_str(ev.change)),
            );
            if let Some(before) = ev.before {
                data.insert("before".to_owned(), before);
            }
            if let Some(after) = ev.after {
                data.insert("after".to_owned(), after);
            }
            (format!("{}.changed", ev.query_id), Value::Object(data))
        }
        EventModeling::PerChange => match ev.change {
            ChangeType::Added => {
                let Some(after) = ev.after else {
                    tracing::warn!(query = %ev.query_id, "dropping Added change without `after`");
                    return None;
                };
                (format!("{}.added", ev.query_id), after)
            }
            ChangeType::Updated => {
                let Some(after) = ev.after else {
                    tracing::warn!(query = %ev.query_id, "dropping Updated change without `after`");
                    return None;
                };
                let mut data = Map::new();
                if let Some(before) = ev.before {
                    data.insert("before".to_owned(), before);
                }
                data.insert("after".to_owned(), after);
                (format!("{}.updated", ev.query_id), Value::Object(data))
            }
            ChangeType::Deleted => {
                let Some(before) = ev.before else {
                    tracing::warn!(query = %ev.query_id, "dropping Deleted change without `before`");
                    return None;
                };
                (format!("{}.deleted", ev.query_id), before)
            }
        },
    };
    Some(EmittedEvent {
        name,
        event_id,
        timestamp,
        data,
    })
}

/// Lightweight `inputSchema` enforcement (no JSON Schema engine available in
/// the pinned dependency set): params must be an object, and for event types
/// that advertise the `changeType` filter its value must be a valid kind.
pub fn validate_event_params(
    filters: &HashMap<String, Arc<ParamFilter>>,
    name: &str,
    params: Option<&Value>,
) -> Result<(), JsonRpcError> {
    let Some(params) = params else { return Ok(()) };
    if params.is_null() {
        return Ok(());
    }
    let Some(obj) = params.as_object() else {
        return Err(JsonRpcError::invalid_params("params must be an object"));
    };
    if filters.contains_key(name) {
        if let Some(ct) = obj.get("changeType") {
            let ok = ct.is_null() || ct.as_str().is_some_and(|s| CHANGE_TYPES.contains(&s));
            if !ok {
                return Err(JsonRpcError::invalid_params(
                    "params.changeType must be one of \"added\", \"updated\", \"deleted\"",
                ));
            }
        }
    }
    Ok(())
}

/// Spawns the configured feed source and the loop forwarding mapped events
/// into the buffer. Returns the forwarding task's handle.
pub fn spawn_feed_pipeline(state: Arc<AppState>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<FeedEvent>(1024);
        let feed = match state.config.feed.kind {
            FeedKind::Mock => {
                let query_id = state
                    .config
                    .feed
                    .query_id
                    .clone()
                    .or_else(|| state.config.queries.first().map(|q| q.id.clone()))
                    .unwrap_or_default();
                let interval =
                    Duration::from_millis(state.config.feed.interval_ms.unwrap_or(2000));
                tokio::spawn(drasi_feed::run_mock_feed(tx, query_id, interval))
            }
            FeedKind::DrasiSse => {
                let Some(url) = state.config.feed.url.clone() else {
                    // Guarded at config load; defensive double-check.
                    tracing::error!("feed.kind = drasiSse requires feed.url; feed disabled");
                    return;
                };
                tokio::spawn(drasi_feed::run_drasi_sse_feed(url, tx))
            }
        };
        let mode = state.config.event_modeling;
        while let Some(feed_event) = rx.recv().await {
            let Some(emitted) = map_feed_event(mode, feed_event) else {
                continue;
            };
            if state.registry.get(&emitted.name).is_some() {
                let name = emitted.name.clone();
                let seq = state.buffer.emit(emitted);
                tracing::trace!(%name, seq, "emitted feed event");
            } else {
                tracing::debug!(name = %emitted.name, "dropping feed event for unregistered event type");
            }
        }
        match feed.await {
            Ok(Ok(())) => tracing::info!("feed source finished"),
            Ok(Err(error)) => tracing::error!(error = format!("{error:#}"), "feed source failed"),
            Err(error) => tracing::error!(%error, "feed task panicked"),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        BufferSettings, FeedSettings, PollSettings, PushSettings, QueryConfig, WebhookSettings,
    };

    fn config(mode: EventModeling, webhook_enabled: bool) -> ServerConfig {
        ServerConfig {
            host: "127.0.0.1".into(),
            port: 0,
            auth_tokens: vec![],
            event_modeling: mode,
            buffer: BufferSettings::default(),
            feed: FeedSettings {
                kind: FeedKind::Mock,
                query_id: Some("q1".into()),
                interval_ms: Some(1000),
                url: None,
            },
            queries: vec![QueryConfig {
                id: "q1".into(),
                description: None,
                payload_schema: None,
            }],
            push: PushSettings::default(),
            poll: PollSettings::default(),
            webhook: WebhookSettings {
                enabled: webhook_enabled,
                ..WebhookSettings::default()
            },
        }
    }

    fn feed_event(change: ChangeType, before: Option<Value>, after: Option<Value>) -> FeedEvent {
        FeedEvent {
            query_id: "q1".into(),
            change,
            before,
            after,
            timestamp: Some(Utc::now()),
            upstream_id: Some("row-1-rev-0".into()),
        }
    }

    #[test]
    fn single_mode_registers_one_changed_type_with_filter() {
        let (defs, filters) = build_event_model(&config(EventModeling::Single, false));
        assert_eq!(defs.len(), 1);
        assert_eq!(defs[0].name, "q1.changed");
        assert_eq!(
            defs[0].delivery,
            vec![DeliveryMode::Poll, DeliveryMode::Push]
        );
        let schema = defs[0].input_schema.as_ref().unwrap();
        assert_eq!(
            schema["properties"]["changeType"]["enum"],
            json!(["added", "updated", "deleted"])
        );
        assert!(filters.contains_key("q1.changed"));
    }

    #[test]
    fn webhook_enabled_adds_delivery_mode() {
        let (defs, _) = build_event_model(&config(EventModeling::Single, true));
        assert!(defs[0].delivery.contains(&DeliveryMode::Webhook));
    }

    #[test]
    fn per_change_mode_registers_three_types_without_filters() {
        let (defs, filters) = build_event_model(&config(EventModeling::PerChange, false));
        let names: Vec<&str> = defs.iter().map(|d| d.name.as_str()).collect();
        assert_eq!(names, vec!["q1.added", "q1.updated", "q1.deleted"]);
        assert!(filters.is_empty());
        assert!(defs.iter().all(|d| d.input_schema.is_none()));
    }

    #[test]
    fn single_mode_data_shape_omits_null_sides() {
        let added = map_feed_event(
            EventModeling::Single,
            feed_event(ChangeType::Added, None, Some(json!({"id": 1}))),
        )
        .unwrap();
        assert_eq!(added.name, "q1.changed");
        assert_eq!(added.event_id, "row-1-rev-0");
        assert_eq!(
            added.data,
            json!({"changeType": "added", "after": {"id": 1}})
        );

        let updated = map_feed_event(
            EventModeling::Single,
            feed_event(
                ChangeType::Updated,
                Some(json!({"id": 1})),
                Some(json!({"id": 1, "x": 2})),
            ),
        )
        .unwrap();
        assert_eq!(
            updated.data,
            json!({"changeType": "updated", "before": {"id": 1}, "after": {"id": 1, "x": 2}})
        );

        let deleted = map_feed_event(
            EventModeling::Single,
            feed_event(ChangeType::Deleted, Some(json!({"id": 1})), None),
        )
        .unwrap();
        assert_eq!(
            deleted.data,
            json!({"changeType": "deleted", "before": {"id": 1}})
        );
    }

    #[test]
    fn per_change_data_shapes() {
        let added = map_feed_event(
            EventModeling::PerChange,
            feed_event(ChangeType::Added, None, Some(json!({"id": 7}))),
        )
        .unwrap();
        assert_eq!(added.name, "q1.added");
        assert_eq!(added.data, json!({"id": 7}));

        let updated = map_feed_event(
            EventModeling::PerChange,
            feed_event(
                ChangeType::Updated,
                Some(json!({"id": 7})),
                Some(json!({"id": 8})),
            ),
        )
        .unwrap();
        assert_eq!(updated.name, "q1.updated");
        assert_eq!(
            updated.data,
            json!({"before": {"id": 7}, "after": {"id": 8}})
        );

        let deleted = map_feed_event(
            EventModeling::PerChange,
            feed_event(ChangeType::Deleted, Some(json!({"id": 7})), None),
        )
        .unwrap();
        assert_eq!(deleted.name, "q1.deleted");
        assert_eq!(deleted.data, json!({"id": 7}));

        assert!(map_feed_event(
            EventModeling::PerChange,
            feed_event(ChangeType::Added, None, None)
        )
        .is_none());
    }

    #[test]
    fn missing_upstream_id_falls_back_to_uuid() {
        let mut ev = feed_event(ChangeType::Added, None, Some(json!({})));
        ev.upstream_id = None;
        let a = map_feed_event(EventModeling::Single, ev.clone()).unwrap();
        let b = map_feed_event(EventModeling::Single, ev).unwrap();
        assert_ne!(a.event_id, b.event_id);
        assert!(uuid::Uuid::parse_str(&a.event_id).is_ok());
    }

    #[test]
    fn change_type_filter_matches_data() {
        let (_, filters) = build_event_model(&config(EventModeling::Single, false));
        let f = filters.get("q1.changed").unwrap();
        let data = json!({"changeType": "added", "after": {}});
        assert!(f(&json!({"changeType": "added"}), &data));
        assert!(!f(&json!({"changeType": "deleted"}), &data));
        assert!(f(&json!({}), &data));
        assert!(f(&json!({"changeType": null}), &data));
        assert!(!f(&json!({"changeType": 3}), &data));
    }

    #[test]
    fn validate_event_params_enforces_change_type_enum() {
        let (_, filters) = build_event_model(&config(EventModeling::Single, false));
        let ok = |v: Value| validate_event_params(&filters, "q1.changed", Some(&v)).is_ok();
        assert!(validate_event_params(&filters, "q1.changed", None).is_ok());
        assert!(ok(json!({})));
        assert!(ok(json!({"changeType": "updated"})));
        assert!(ok(json!({"changeType": null})));
        assert!(ok(json!({"otherParam": 42})));
        assert!(!ok(json!({"changeType": "bogus"})));
        assert!(!ok(json!({"changeType": 1})));
        assert!(!ok(json!("not-an-object")));
        // No filter registered for unknown names: only the object check applies.
        assert!(validate_event_params(&filters, "other", Some(&json!({"changeType": "x"}))).is_ok());
    }
}
