import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonOutput
import java.time.Instant
import java.util.UUID

def Message processData(Message message) {
    def properties = message.getProperties()
    def ex = properties.get("CamelExceptionCaught")
    
    String rawError = ex ? ex.getMessage() : "Kafka Stream Deserialization or Aggregation Failure on Edge Node"
    String rawStream = properties.get("Original_Raw_Stream") ? properties.get("Original_Raw_Stream").toString() : "[]"
    String corrId = (properties.get("CorrelationID") ?: UUID.randomUUID().toString()).toString()
    String edgeNode = (properties.get("Edge_Node_ID") ?: "EIC-K8S-BERLIN-NODE-01").toString()

    def dlqEnvelope = [
        "DLQ_Routing": [
            "IncidentCategory"  : "EDGE_KAFKA_STREAM_EXCEPTION",
            "Timestamp"         : Instant.now().toString(),
            "CorrelationId"     : corrId,
            "EdgeNodeIdentifier": edgeNode,
            "DeadLetterQueue"   : "amqp/dlq/retail/kafka/quarantine"
        ],
        "Diagnostic": [
            "RootCause"         : rawError,
            "RecommendedAction" : "Validate incoming Kafka batch schema or consumer offset state on Kubernetes Edge Node"
        ],
        "FailedStreamPayload"   : rawStream
    ]

    message.setBody(JsonOutput.prettyPrint(JsonOutput.toJson(dlqEnvelope)))
    message.setHeader("Content-Type", "application/json")
    message.setHeader("X-Stream-Status", "QUARANTINED_TO_AMQP_DLQ")
    
    return message
}
