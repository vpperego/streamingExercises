
import java.util.{Date, Properties}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.Source
import scala.util.Random

object kafkaProducer extends App {
  if (args.length < 3){
    println("Usage: kafkaProducer dataset topic")
    System.exit(1)
  }
  val events = 20
  val topic = args(2)
  val brokers = "localhost:9092"
  val rnd = new Random()
  val props = new Properties()
  props.put("bootstrap.servers", brokers)
  props.put("client.id", "ScalaProducerExample")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")


  val producer = new KafkaProducer[String, String](props)
  val t = System.currentTimeMillis()

  val bufferedSource = Source.fromFile(args(1))
  for (line <- bufferedSource.getLines) {
    val data = new ProducerRecord[String, String](topic, line)
    producer.send(data)

  }
  System.out.println("sent per second: " + events * 1000 / (System.currentTimeMillis() - t))
  producer.close()
}