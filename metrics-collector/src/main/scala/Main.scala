import org.apache.kafka.clients.consumer.{KafkaConsumer, ConsumerConfig}
import scala.collection.JavaConverters._

@main def main(): Unit ={
  println("Metrics collector unit")
  val kafkaServer = sys.env.getOrElse("KAFKA_SERVER", "localhost:9092")
  val inserterTopic = sys.env.getOrElse("INSERTER_METRICS_TOPIC", "inserter-metrics")
  val partitions = sys.env.getOrElse("TOPIC_PARTITIONS", "1").toInt
  val logs = sys.env.getOrElse("LOGS_ENABLED", "false").toBoolean

  val props = getProps(kafkaServer)

  val kafkaConsumer = new KafkaConsumer[String, String](props)
  kafkaConsumer.subscribe(List(inserterTopic).asJava)

  val inserterConsumer = new Consumers.InserterConsumer(kafkaConsumer, java.time.Duration.ofMillis(100), logs)
  val thread = inserterConsumer.thread
  thread.start()


  Runtime.getRuntime().addShutdownHook(new Thread() {
    override def run(): Unit = {
      println("Shutting down gracefully...")
      kafkaConsumer.wakeup()
      thread.join()
      println(s"Latest Burst Start: ${inserterConsumer.latestBurstStart}")
      println(s"Latest Burst End: ${inserterConsumer.latestBurstEnd}")
      
      println("Shutdown complete.")
    }
  })

  thread.join()
}

def getProps(kafkaServer: String): java.util.Properties = {
  val props = new java.util.Properties()
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer)
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ConsumerConfig.GROUP_ID_CONFIG, "metrics-collector-group")

  props
}


