# Enterprise Integration Architecture: Hybrid Edge Integration Cell & Kafka Event Streaming (Retail & E-commerce)

[![SAP Integration Suite](https://img.shields.io/badge/SAP%20Integration%20Suite-Cloud%20Integration-0A85EA?style=for-the-badge&logo=sap&logoColor=white)](https://www.sap.com)
[![Edge Computing](https://img.shields.io/badge/Runtime-Edge%20Integration%20Cell%20(EIC)-008080?style=for-the-badge&logo=kubernetes&logoColor=white)](https://www.sap.com)
[![Event Streaming](https://img.shields.io/badge/Streaming-Apache%20Kafka%20%7C%20AMQP-red?style=for-the-badge&logo=apachekafka&logoColor=white)](https://kafka.apache.org)
[![Advanced Event Mesh](https://img.shields.io/badge/Broker-SAP%20Advanced%20Event%20Mesh-purple?style=for-the-badge&logo=sap&logoColor=white)](https://www.sap.com)
[![Clean Core](https://img.shields.io/badge/Clean%20Core-EDA%20%7C%20Zero%20ERP%20Overload-success?style=for-the-badge&logo=sap&logoColor=white)](https://www.sap.com)

---

## Executive Summary

Global omni-channel retail platforms and high-volume e-commerce brands generate tens of thousands of inventory updates per minute during promotional peaks (e.g., flash sales, Black Friday). Pushing point-to-point updates synchronously into SAP S/4HANA causes severe thread lock, queue exhaustion, and prohibitive cloud egress latency.

This architecture establishes a **Hybrid Edge Event-Driven Architecture (EDA)**:
1. **Edge Execution:** Integration logic runs inside private Kubernetes clusters on-premise using **SAP Edge Integration Cell (EIC)**, achieving single-digit millisecond latency and data sovereignty.
2. **High-Throughput Streaming:** Ingests micro-batches directly from **Apache Kafka** clusters.
3. **In-Flight Aggregation:** A streaming Groovy engine performs delta-consolidation per `SKU / Plant / Storage Location`, reducing transactional ERP volume by over 85%.
4. **Guaranteed Delivery:** Emits CloudEvents 1.0 payloads via AMQP into **SAP Advanced Event Mesh (AEM)** for resilient, decoupled enterprise propagation.

---

## Architectural Topology

```
[E-Commerce Platform / Point-of-Sale Terminals]
       │
       │ High-Velocity Inventory Changes (10,000 msg/sec)
       ▼
[Apache Kafka Cluster (Topic: retail.inventory.events)]
       │
       │ Distributed Partition Consumer
       ▼
[SAP Edge Integration Cell (EIC on Kubernetes Cluster)]
 │
 ├── 1. Ingestion Layer (/retail/edge/kafka/inventory/stream):
 │    └─ Content Modifier: CM_Init_Edge_Stream
 │         ├─ Preserves raw micro-batch payload
 │         └─ Injects correlation and cluster telemetry (EIC-K8S-BERLIN-NODE-01)
 │
 ├── 2. High-Performance Stream Aggregator:
 │    └─ Groovy Script: Aggregate_Kafka_Inventory_Stream.groovy
 │         ├─ Consolidates item delta adjustments by compound key (SKU+Plant+LGORT)
 │         ├─ Detects negative breach thresholds and tags critical alerts
 │         └─ Reduces downstream payload size by 85%+
 │
 ├── 3. CloudEvents 1.0 Envelope Formulation:
 │    └─ Content Modifier: CM_Format_AEM_CloudEvents
 │         ├─ Adds specversion=1.0, ce-type=sap.retail.inventory.aggregated.v1
 │         └─ Injects AMQP routing headers
 │
 ├── 4. Broker Propagation:
 │    └─ AMQP 1.0 Adapter -> SAP Advanced Event Mesh (Topic: retail/inventory/realtime/updates/v1)
 │
 └── 5. Edge Dead-Letter Queue (DLQ):
      ├─ Isolates schema corruptions or partition offset de-syncs
      ├─ Groovy Formatter: Format_Kafka_Edge_DLQ.groovy
      └─ Outbound AMQP Quarantine -> amqp/dlq/retail/kafka/quarantine
```

---

## Technical Specifications

| Architectural Dimension | Traditional Cloud Integration | Hybrid Edge Architecture (EIC + AEM) |
| :--- | :--- | :--- |
| **Execution Runtime** | Public Multi-Tenant Cloud (SAP BTP) | Local Kubernetes Cluster (Edge Integration Cell) |
| **Ingestion Protocol** | Polled HTTP / Webhook | Native Apache Kafka Partition Consumer / AMQP 1.0 |
| **Message Handling** | Sequential transactional posting | High-throughput in-memory micro-batch aggregation |
| **Network Latency** | 120ms - 350ms (Internet WAN) | < 5ms (Private LAN / Cluster Edge) |
| **Decoupling Broker** | Classical JMS Queues | SAP Advanced Event Mesh (Dynamic Event Routing) |
| **ERP Impact** | Synchronous API locks & thread exhaustion | Asynchronous consolidated delta inventory postings |

---

## Aggregated Target Data Contract (CloudEvents 1.0)

```json
{
  "EdgeStreamMetadata": {
    "RuntimeEngine": "EDGE_INTEGRATION_CELL",
    "NodeCluster": "EIC-K8S-BERLIN-NODE-01",
    "MessageBroker": "APACHE_KAFKA_STREAMING",
    "TargetEventMesh": "SAP_ADVANCED_EVENT_MESH_AMQP",
    "TotalIngestedEvents": 3,
    "DistinctSKUsCount": 2,
    "CriticalAlerts": 1,
    "AggregationWindow": "REALTIME_MICRO_BATCH_100MS"
  },
  "AggregatedInventoryUpdates": [
    {
      "SKU": "MAT-LUX-9901",
      "Plant": "1010",
      "StorageLocation": "0001",
      "NetQuantityDelta": 120.0,
      "EventCount": 2,
      "LastTimestamp": "2026-11-07T19:30:02Z",
      "CriticalLowStock": false
    },
    {
      "SKU": "MAT-LUX-4420",
      "Plant": "1020",
      "StorageLocation": "0002",
      "NetQuantityDelta": -500.0,
      "EventCount": 1,
      "LastTimestamp": "2026-11-07T19:30:05Z",
      "CriticalLowStock": true
    }
  ]
}
```

---

## Repository Structure

```
├── README.md                                          # Architectural specification and operations guide
├── ADR-001-Edge-Integration-Cell-Kafka-Streaming.md   # Architecture Decision Record
├── IF_Edge_Kafka_Inventory_Aggregation_AEM.zip        # Production iFlow runtime bundle export
├── cpi/
│   └── scripts/
│       ├── Aggregate_Kafka_Inventory_Stream.groovy    # High-throughput micro-batch stream aggregator
│       └── Format_Kafka_Edge_DLQ.groovy               # Edge dead-letter queue incident handler
├── tests/
│   └── sample_kafka_inventory_stream.json             # High-concurrency event stream test payload
└── .gitignore                                         # Git exclusion rules
```

---

## Verification & Deployment Runbook

### 1. Inbound Endpoint Verification
Verify runtime status in **Monitor** ──► **Manage Integration Content**:
```text
https://<tenant-runtime>.cfapps.<region>[.hana.ondemand.com/http/retail/edge/kafka/inventory/stream](https://.hana.ondemand.com/http/retail/edge/kafka/inventory/stream)
```

### 2. Dispatched cURL Test
Execute an authorized POST request using sample micro-batch records:

```bash
curl -X POST \
  'https://<tenant-runtime>.cfapps.<region>[.hana.ondemand.com/http/retail/edge/kafka/inventory/stream](https://.hana.ondemand.com/http/retail/edge/kafka/inventory/stream)' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <BTP_OAUTH_TOKEN>' \
  --data-binary '@tests/sample_kafka_inventory_stream.json'
```

### 3. Monitoring Trace
* Inspect message execution in **Monitor Message Processing**.
* Verify aggregation counters (`IngestedEventsCount`, `DistinctSKUs`).
* Confirm response payload contains consolidated net inventory delta and AMQP broker confirmation.

---

## Author

**Željko Popov**  
*Integration Architect | SAP Integration Suite & Clean Core Specialist*  
[GitHub Repositories](https://github.com/neurozeljko)
