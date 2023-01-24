package io.confluent.examples.flink.scalastreams

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.streaming.api.functions.sink.SinkFunction

class ShipmentLogger extends SinkFunction[String] with LazyLogging {

  val mapper = new ObjectMapper()

  override def invoke(shipmentString: String, context: SinkFunction.Context): Unit = {
    val shipment = mapper.readValue[Shipment](shipmentString, classOf[Shipment])
    logger.info(shipmentString)
    logger.info("time since order: {} ", System.currentTimeMillis() - shipment.order.ordertime)
  }

}
