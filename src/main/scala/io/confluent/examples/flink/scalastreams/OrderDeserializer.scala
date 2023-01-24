package io.confluent.examples.flink.scalastreams

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.serialization.{Deserializer, Serializer}

class OrderDeserializer extends Deserializer[Order] {
  override def deserialize(topic: String, data: Array[Byte]): Order = {
    new ObjectMapper().readValue(data, classOf[Order])
  }
}


