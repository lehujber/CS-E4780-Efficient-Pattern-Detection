import com.github.tototoshi.csv.CSVReader
import org.apache.kafka.clients.producer.{KafkaProducer , ProducerConfig, ProducerRecord}
import java.util.Properties
import java.io.File
import java.time.LocalDateTime

@main def main(): Unit ={
  val kafkaServer = sys.env.getOrElse("KAFKA_SERVER", "localhost:9092")
  val topicName = sys.env.getOrElse("TOPIC_NAME", "citibike-trips")
  val partitions = sys.env.getOrElse("TOPIC_PARTITIONS", "1").toInt

  val logTrips = sys.env.getOrElse("LOG_TRIPS", "false").toBoolean

  val burstSize = sys.env.getOrElse("BURST_SIZE", "1").toInt
  val burstMinSleepMs = sys.env.getOrElse("BURST_MIN_SLEEP_MS", "0").toInt

  val dataFolderPath = sys.env.getOrElse("DATA_FOLDER_PATH", "/data/")
  val dir = File(dataFolderPath)
  val files = dir.listFiles
    .filter(f => f.isFile && f.getName.endsWith(".csv"))
    .map(_.getAbsolutePath)

  val readers = files.map(file => CSVReader.open(file))

  val props = Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.ACKS_CONFIG, "all")

  val producer = KafkaProducer(props)

  val inserters = readers.map(reader => KafkaInserter(producer, topicName, partitions, logTrips, burstSize, burstMinSleepMs, reader))

  val count = inserters.map(_.run()).sum

  println("Produces successfully finished sending data to Kafka")
  println(s"Total number of messages send: ${count}")
  println(s"Number of files processed: ${files.length}")
}

case class KafkaInserter(
  kafka: KafkaProducer[Object,Object],
  topicName: String, 
  partitions: Int, 
  logTrips: Boolean, 
  burstSize: Int, 
  burstMinSleepMs: Int,
  reader: CSVReader
  ){
  
  def run(): BigInt = {
    println(s"Logging trips: $logTrips")
    println(s"Burst size: $burstSize, Burst min sleep ms: $burstMinSleepMs")

    val rowIterator = reader.iterator
    rowIterator.next()
    // sendMessage(rowIterator.next()) 

    var count: BigInt = 0
    for (group <- rowIterator.grouped(burstSize)) {
      count += group.size
      group.foreach(rec => sendMessage(rec))
      println("Timeout before sending next burst...")
      Thread.sleep(burstMinSleepMs)
    }

    count
  }

  def sendMessage(rec: Seq[String]): Unit = {
    try {
      val msg = rec.mkString(", ")
      kafka.send(ProducerRecord(topicName,msg))
      if (logTrips) {
        println(s"Sent message: $msg")
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }
}