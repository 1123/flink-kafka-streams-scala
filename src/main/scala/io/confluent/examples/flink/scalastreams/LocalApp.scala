package io.confluent.examples.flink.scalastreams

import java.util.Properties

object LocalApp extends
  KafkaPropsComponent with
  StreamsAppComponent with Serializable {

  val kafkaProps = new Properties()
  kafkaProps.put("bootstrap.servers", "localhost:9092")
  val streamsApp = new StreamsApp

  def main(args: Array[String]): Unit = {
    streamsApp.run()
  }
}
