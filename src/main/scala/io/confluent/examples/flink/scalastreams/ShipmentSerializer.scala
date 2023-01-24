package io.confluent.examples.flink.scalastreams

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.serialization.Serializer

class ShipmentSerializer extends Serializer[Shipment] {
  override def serialize(topic: String, shipment: Shipment): Array[Byte] = {
    new ObjectMapper().writeValueAsBytes(shipment)
  }
}
