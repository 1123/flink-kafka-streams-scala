package io.confluent.examples.flink.scalastreams

import org.junit.jupiter.api.Test

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.util.Properties

class SerializationTest extends StreamsAppComponent with KafkaPropsComponent with Serializable {

  val kafkaProps = new Properties()
  val streamsApp = new StreamsApp

  @Test
  def testSerialization(): Unit = {
    val baos = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(kafkaProps)
    oos.writeObject(streamsApp)
  }

}


