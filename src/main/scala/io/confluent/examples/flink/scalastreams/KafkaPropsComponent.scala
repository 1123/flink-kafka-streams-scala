package io.confluent.examples.flink.scalastreams

import java.util.Properties

trait KafkaPropsComponent {
  @transient
  val kafkaProps: Properties

}
