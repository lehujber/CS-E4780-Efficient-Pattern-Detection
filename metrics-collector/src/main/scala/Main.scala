import org.apache.kafka.clients.consumer.{KafkaConsumer, ConsumerConfig}
import scala.collection.JavaConverters._

@main def main(): Unit ={
  val kafkaServer = sys.env.getOrElse("KAFKA_SERVER", "localhost:9092")
  val inserterTopic = sys.env.getOrElse("INSERTER_METRICS_TOPIC", "inserter-metrics")
  val metricsTopic = sys.env.getOrElse("METRICS_TOPIC", "metrics")
  val matchesTopic = sys.env.getOrElse("MATCHES_TOPIC", "matches")
  val partitions = sys.env.getOrElse("TOPIC_PARTITIONS", "1").toInt

  val logBursts = sys.env.getOrElse("LOG_BURSTS", "false").toBoolean

  println("Metrics collector unit started...")

  val props = getProps(kafkaServer)

  val kafkaConsumer = new KafkaConsumer[String, String](props)
  kafkaConsumer.subscribe(List(inserterTopic).asJava)

  val inserterConsumer = new Consumers.InserterConsumer(kafkaConsumer, java.time.Duration.ofMillis(100))
  val thread = inserterConsumer.thread

  val matchesKafkaConsumer = new KafkaConsumer[String, String](props)
  matchesKafkaConsumer.subscribe(List(matchesTopic).asJava)
  val matchesConsumer = new Consumers.MatchesConsumer(matchesKafkaConsumer, java.time.Duration.ofMillis(100))
  val matchesThread = matchesConsumer.thread

  Runtime.getRuntime().addShutdownHook(new Thread() {
    override def run(): Unit = {
      println("Shutting down gracefully...")
      kafkaConsumer.wakeup()
      thread.join()
      println(s"Latest Burst Start: ${inserterConsumer.latestBurstStart}")
      println(s"Latest Burst End: ${inserterConsumer.latestBurstEnd}")
      println(s"Total Matches: ${matchesConsumer.totalMatches}")

      println("Shutdown complete.")
    }
  })

  matchesThread.start()
  if (logBursts) {
    thread.start()
    thread.join()
  }
  matchesThread.join()
}

def getProps(kafkaServer: String): java.util.Properties = {
  val props = new java.util.Properties()
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer)
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ConsumerConfig.GROUP_ID_CONFIG, "metrics-collector-group")

  props
}


