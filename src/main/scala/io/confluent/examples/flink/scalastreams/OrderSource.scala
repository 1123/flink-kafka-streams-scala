package io.confluent.examples.flink.scalastreams

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.util.Random

class OrderSource extends LazyLogging with SourceFunction[Order] {

  val random = new Random()
  var isRunning = true

  override def run(ctx: SourceFunction.SourceContext[Order]): Unit = {
    while (isRunning) {
      logger.info("Creating an Order")
      ctx.collect(Order(
        System.currentTimeMillis(),
        random.nextInt(1000),
        random.nextInt(100) + "",
        Address("Some City", "Some State", random.nextInt(100000)))
      )
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }

}
