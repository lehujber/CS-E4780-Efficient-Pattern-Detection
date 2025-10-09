import org.apache.kafka.clients.consumer.{KafkaConsumer, ConsumerConfig}
import scala.collection.JavaConverters._

import com.typesafe.scalalogging.Logger
import org.slf4j.event.Level
import org.slf4j.LoggerFactory

@main def main(): Unit ={
  val kafkaServer = sys.env.getOrElse("KAFKA_SERVER", "localhost:9092")
  val inserterTopic = sys.env.getOrElse("INSERTER_METRICS_TOPIC", "inserter-metrics")
  val partitions = sys.env.getOrElse("TOPIC_PARTITIONS", "1").toInt
  val logLevel = sys.env.getOrElse("LOG_LEVEL", "INFO").toString

  val level = logLevel.toUpperCase() match {
    case "TRACE" => Level.TRACE
    case "DEBUG" => Level.DEBUG
    case "INFO"  => Level.INFO
    case "WARN"  => Level.WARN
    case "ERROR" => Level.ERROR
    case _       => Level.INFO
  }

  val logger = LoggerFactory.getLogger("main").atLevel(level).asInstanceOf[Logger]
  logger.info("Metrics collector unit started...")

  val props = getProps(kafkaServer)

  val kafkaConsumer = new KafkaConsumer[String, String](props)
  kafkaConsumer.subscribe(List(inserterTopic).asJava)

  val inserterConsumer = new Consumers.InserterConsumer(kafkaConsumer, java.time.Duration.ofMillis(100), level)
  val thread = inserterConsumer.thread
  thread.start()



  Runtime.getRuntime().addShutdownHook(new Thread() {
    override def run(): Unit = {
      val logger = Logger("ShutdownHook")
      logger.info("Shutting down gracefully...")
      kafkaConsumer.wakeup()
      thread.join()
      logger.info(s"Latest Burst Start: ${inserterConsumer.latestBurstStart}")
      logger.info(s"Latest Burst End: ${inserterConsumer.latestBurstEnd}")

      logger.info("Shutdown complete.")
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


