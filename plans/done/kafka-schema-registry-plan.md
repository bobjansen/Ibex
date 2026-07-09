# Kafka Schema Registry Plan

This is the concrete follow-on plan for finishing Schema Registry-backed Avro
support in the Kafka plugin.

## Goal

Keep the existing demo shape:

- Redpanda produces events into `ticks`
- Ibex consumes from Kafka directly
- Ibex transforms the stream
- Ibex pushes results to WebSocket dashboards

But add a Schema Registry-backed Avro path alongside the existing JSON path.

## Scope decisions

- Target Redpanda Schema Registry, not Confluent-specific services.
- No sidecar decode service.
- Flat-record support first.
- No nested/repeated/union-heavy generality in v1.
- Keep the current JSON path intact.
- Shelve Protobuf for now; do not spend implementation effort on it until Avro
  is hardened and there is a concrete user need.

## Work items

### Phase 1: shared registry/wire scaffolding

- [x] Add schema-registry project plan to the repo.
- [x] Parse Schema Registry wire envelopes (`magic byte` + `schema id` + payload).
- [x] Parse Redpanda Schema Registry schema responses.
- [x] Add tests for envelope parsing and response parsing.

### Phase 2: registry client

- [x] Add a small HTTP client layer for Schema Registry lookups.
- [x] Cache schema lookups by schema id.
- [x] Handle transient registry errors without killing long-lived stream jobs.

### Phase 3: Avro v1

- [x] Vendor Avro C++ in the Kafka plugin build so Avro support does not depend on `avro-c`.
- [x] Add `kafka_recv_avro(...)`.
- [x] Decode flat Avro records from Schema Registry-backed Kafka messages.
- [x] Map Avro scalars/logical timestamps/dates into Ibex columns.
- [x] Add Avro-focused tests.
- [x] Add one live Avro demo producer path.
- [x] Reject unsupported Avro features with clear runtime errors.
- [ ] Re-run and document the end-to-end Avro demo after the final hardening pass.

### Phase 4: Protobuf v1 - shelved

No active work. Revisit only after Avro is stable and there is a specific
use case that justifies adding a second Schema Registry payload decoder.

### Phase 5: docs and demo polish

- [x] Extend the Kafka/Redpanda website docs with Avro examples.
- [x] Add terminal viewers / dashboard notes for the Avro demo path.
- [x] Document the supported type subset and explicit v1 limitations.
- [ ] Remove or rewrite any remaining wording that implies Protobuf is part of
  the current plan.

## Suggested order

1. Re-run the JSON and Avro dashboard demos end to end.
2. Clean up remaining Protobuf-forward wording so the docs reflect the shelved scope.
