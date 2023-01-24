package io.confluent.examples.flink.scalastreams

import java.util.Properties

object ConfluentCloudApp extends
  KafkaPropsComponent with
  StreamsAppComponent with Serializable {

  val kafkaProps = new Properties()
  // TODO: could also use require.
  assert(
    System.getenv("BOOTSTRAP_SERVERS") != null,
    "You must set the BOOTSTRAP_SERVERS environment variable. "
  )
  kafkaProps.put("bootstrap.servers", System.getenv("BOOTSTRAP_SERVERS"))
  kafkaProps.put("ssl.endpoint.identification.algorithm", "https")
  kafkaProps.put("sasl.mechanism", "PLAIN")
  kafkaProps.put("security.protocol", "SASL_SSL")
  assert(
    System.getenv("SASL_JAAS_CONFIG") != null,
    "You must set the SASL_JAAS_CONFIG environment variable. "
  )
  kafkaProps.put("sasl.jaas.config", System.getenv("SASL_JAAS_CONFIG"))

  val streamsApp = new StreamsApp

  def main(args: Array[String]): Unit = {
    streamsApp.run()
  }
}
