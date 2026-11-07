import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.time.Instant

def Message processData(Message message) {
    String body = message.getBody(String.class)
    def jsonSlurper = new JsonSlurper()
    def incomingData = jsonSlurper.parseText(body)

    def eventsList = incomingData instanceof List ? incomingData : [incomingData]
    def aggregatedStockMap = [:]
    int totalEventsProcessed = eventsList.size()
    int stockBreachAlerts = 0

    eventsList.each { event ->
        String sku = (event.SKU ?: event.MaterialID ?: "UNKNOWN_SKU").toString().trim().toUpperCase()
        String plant = (event.Plant ?: "1010").toString().trim().toUpperCase()
        String storageLoc = (event.StorageLocation ?: "0001").toString().trim().toUpperCase()
        double deltaQty = 0.0
        
        try {
            deltaQty = Double.parseDouble(event.DeltaQuantity?.toString() ?: "0.0")
        } catch (Exception ex) {
            deltaQty = 0.0
        }

        String aggregationKey = "${sku}|${plant}|${storageLoc}"

        if (!aggregatedStockMap.containsKey(aggregationKey)) {
            aggregatedStockMap[aggregationKey] = [
                "SKU"             : sku,
                "Plant"           : plant,
                "StorageLocation" : storageLoc,
                "NetQuantityDelta": 0.0,
                "EventCount"      : 0,
                "LastTimestamp"   : event.Timestamp ?: Instant.now().toString(),
                "CriticalLowStock": false
            ]
        }

        def record = aggregatedStockMap[aggregationKey]
        record.NetQuantityDelta += deltaQty
        record.EventCount += 1
        
        if (record.NetQuantityDelta < 0.0) {
            record.CriticalLowStock = true
            stockBreachAlerts++
        }
    }

    def aggregatedPayload = [
        "EdgeStreamMetadata": [
            "RuntimeEngine"       : "EDGE_INTEGRATION_CELL",
            "NodeCluster"         : message.getProperty("Edge_Node_ID") ?: "EIC-K8S-BERLIN-NODE-01",
            "MessageBroker"       : "APACHE_KAFKA_STREAMING",
            "TargetEventMesh"     : "SAP_ADVANCED_EVENT_MESH_AMQP",
            "TotalIngestedEvents" : totalEventsProcessed,
            "DistinctSKUsCount"   : aggregatedStockMap.size(),
            "CriticalAlerts"      : stockBreachAlerts,
            "AggregationWindow"   : "REALTIME_MICRO_BATCH_100MS"
        ],
        "AggregatedInventoryUpdates": aggregatedStockMap.values().toList()
    ]

    message.setProperty("IngestedEventsCount", totalEventsProcessed)
    message.setProperty("DistinctSKUs", aggregatedStockMap.size())
    message.setProperty("CriticalAlertsCount", stockBreachAlerts)
    
    message.setBody(JsonOutput.prettyPrint(JsonOutput.toJson(aggregatedPayload)))
    message.setHeader("Content-Type", "application/json")
    message.setHeader("X-Edge-Processed", "true")

    return message
}
