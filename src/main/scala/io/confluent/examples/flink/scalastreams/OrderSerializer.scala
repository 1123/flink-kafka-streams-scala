package io.confluent.examples.flink.scalastreams

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.serialization.Serializer

class OrderSerializer extends Serializer[Order] {
  override def serialize(topic: String, order: Order): Array[Byte] = {
    new ObjectMapper().writeValueAsBytes(order)
  }
}
