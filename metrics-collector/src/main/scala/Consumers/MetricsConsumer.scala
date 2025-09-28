package Consumers

import scala.collection.JavaConverters._
import org.apache.kafka.clients.consumer.KafkaConsumer

class MetricsConsumer(
  kafka: KafkaConsumer[String, String],
  timeout: java.time.Duration
){
  def read(): Seq[String] = {
    val recs = kafka.poll(timeout)

    return recs.iterator().asScala.toSeq.map(_.value)
  }

  def run(): Unit={}

  def thread = new Thread(new Runnable {
    override def run(): Unit = MetricsConsumer.this.run()
  }){
    this.setDaemon(true)
  }
}
