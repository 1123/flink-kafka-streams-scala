package io.confluent.examples.flink.scalastreams

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector

class OrderDispatcher extends ProcessFunction[Order, Shipment] {
  override def processElement(order: Order,
                              context: ProcessFunction[Order, Shipment]#Context,
                              collector: Collector[Shipment]): Unit = {
    collector.collect(Shipment(order, System.currentTimeMillis()))
  }
}

