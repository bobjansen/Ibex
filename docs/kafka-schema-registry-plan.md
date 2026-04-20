# Kafka Schema Registry Plan

This is the concrete follow-on plan for extending the current JSON-only Kafka
plugin to support Avro and Protobuf payloads through Redpanda Schema Registry.

## Goal

Keep the existing demo shape:

- Redpanda produces events into `ticks`
- Ibex consumes from Kafka directly
- Ibex transforms the stream
- Ibex pushes results to WebSocket dashboards

But replace JSON payloads with Schema Registry-backed Avro and Protobuf.

## Scope decisions

- Target Redpanda Schema Registry, not Confluent-specific services.
- No sidecar decode service.
- Flat-record support first.
- No nested/repeated/union-heavy generality in v1.
- Keep the current JSON path intact.

## Work items

### Phase 1: shared registry/wire scaffolding

- [x] Add schema-registry project plan to the repo.
- [ ] Parse Schema Registry wire envelopes (`magic byte` + `schema id` + payload).
- [ ] Parse Redpanda Schema Registry schema responses.
- [ ] Add tests for envelope parsing and response parsing.

### Phase 2: registry client

- [ ] Add a small HTTP client layer for Schema Registry lookups.
- [ ] Cache schema lookups by schema id.
- [ ] Handle transient registry errors without killing long-lived stream jobs.

### Phase 3: Avro v1

- [ ] Add `kafka_recv_avro(...)`.
- [ ] Decode flat Avro records from Schema Registry-backed Kafka messages.
- [ ] Map Avro scalars/logical timestamps/dates into Ibex columns.
- [ ] Reject unsupported Avro features with clear runtime errors.
- [ ] Add Avro-focused tests and one live demo producer path.

### Phase 4: Protobuf v1

- [ ] Add `kafka_recv_protobuf(...)`.
- [ ] Decode flat Protobuf messages from Schema Registry-backed Kafka messages.
- [ ] Support explicit message type selection when needed.
- [ ] Map enums/logical scalars into Ibex columns.
- [ ] Reject nested/repeated/oneof-heavy cases in v1.
- [ ] Add Protobuf-focused tests and one live demo producer path.

### Phase 5: docs and demo polish

- [ ] Extend the Kafka/Redpanda website docs with Avro/Protobuf examples.
- [ ] Add terminal viewers / dashboard notes for the Avro/Protobuf demo path.
- [ ] Document the supported type subset and explicit v1 limitations.

## Suggested order

1. Finish the shared registry/wire scaffolding.
2. Build Avro end-to-end first.
3. Reuse the same transport/registry path for Protobuf.

