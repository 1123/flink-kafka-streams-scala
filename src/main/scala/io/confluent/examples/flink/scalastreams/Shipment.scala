package io.confluent.examples.flink.scalastreams

import scala.beans.BeanProperty

case class Shipment(
                     @BeanProperty order: Order,
                     @BeanProperty time: Long
                   ) {
  def this() = this(null, 0L)
}
