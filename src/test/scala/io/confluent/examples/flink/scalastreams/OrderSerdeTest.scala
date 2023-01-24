package io.confluent.examples.flink.scalastreams

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class OrderSerdeTest {

  @Test
  def theSerdesShouldRoundTripAnOrderObjectWithNestedAddressObject(): Unit = {
    val order = Order(10000L, 3, "foo", Address("Munich", "Bayern", 80333))
    val serialized = new OrderSerializer().serialize("someTopic", order)
    print(new String(serialized))
    val deserialized = new OrderDeserializer().deserialize("someTopic", serialized)
    assertEquals(order, deserialized)
  }

}


