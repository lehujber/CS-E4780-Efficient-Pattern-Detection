package Consumers

import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration
import java.time.LocalDateTime
import java.util.concurrent.atomic.AtomicReference

class InserterConsumer(kafka: KafkaConsumer[String, String], timeout: Duration) extends MetricsConsumer(kafka, timeout) {
    private val _latestBurstStart = AtomicReference[LocalDateTime](LocalDateTime.MIN)
    private val _latestBurstEnd = AtomicReference[LocalDateTime](LocalDateTime.MIN)

    private def updateBurstStart(newStart: LocalDateTime): Unit = {
        _latestBurstStart.updateAndGet { current =>
            if (newStart.isAfter(current)) newStart else current
        }
    }

    private def updateBurstEnd(newEnd: LocalDateTime): Unit = {
        _latestBurstEnd.updateAndGet { current =>
            if (newEnd.isAfter(current)) newEnd else current
        }
    }

    def latestBurstStart: LocalDateTime = _latestBurstStart.get
    def latestBurstEnd: LocalDateTime = _latestBurstEnd.get
    override def run(): Unit ={
        while (true) {
            val recs = read()
            recs.foreach{ rec =>
                try{
                    val parts = rec.split(",")
                    if(parts.length == 2){
                        val burstStart = LocalDateTime.parse(parts(0).trim)
                        val burstEnd = LocalDateTime.parse(parts(1).trim)
                        println(s"Received burst: start=${burstStart.toString()}, end=${burstEnd.toString()}")
                        updateBurstStart(burstStart)
                        updateBurstEnd(burstEnd)
                    }
                }catch {
                    case e: Exception => println(s"Error parsing record: {$rec}, error: ${e.getMessage()}")
                }
            }
        }
    }
}
