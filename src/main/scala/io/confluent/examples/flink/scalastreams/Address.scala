package io.confluent.examples.flink.scalastreams

import scala.beans.BeanProperty

case class Address(
                    @BeanProperty city: String,
                    @BeanProperty state: String,
                    @BeanProperty zipcode: Int
                  ) {
  def this() = this(null, null, 0)
}
