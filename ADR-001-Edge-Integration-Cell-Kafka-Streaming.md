# ADR-001: Localized Micro-Batch Aggregation via SAP Edge Integration Cell (EIC) and Kafka

## Status
Accepted

## Context
High-concurrency e-commerce operations overload ERP posting queues when point-of-sale inventory updates are dispatched directly across public internet WAN links.

## Decision
1. **Edge Deployment:** Deploy Cloud Integration runtime inside on-premise Kubernetes clusters via SAP Edge Integration Cell (EIC).
2. **In-Flight Aggregation:** Stream incoming Kafka topic micro-batches into consolidated net deltas grouped by SKU and storage location.
3. **Decoupled Propagation:** Route aggregated CloudEvents 1.0 packages through SAP Advanced Event Mesh using AMQP 1.0 protocols.
4. **Resilience:** Direct partition parsing exceptions and serialization faults into an isolated AMQP Dead-Letter Queue without blocking the streaming pipeline.
