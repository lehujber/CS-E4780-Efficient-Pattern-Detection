package Consumers

import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration
import java.time.LocalDateTime
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.atomic.AtomicInteger

class MatchesConsumer(kafka: KafkaConsumer[String, String], timeout: Duration) extends MetricsConsumer(kafka, timeout) {
    private val _latestBurstStart = AtomicReference[LocalDateTime](LocalDateTime.MIN)
    private val _latestBurstEnd = AtomicReference[LocalDateTime](LocalDateTime.MIN)

    private def updateBurstStart(newStart: LocalDateTime): Unit = {
        _latestBurstStart.updateAndGet { current =>
            if (newStart.isAfter(current)) newStart else current
        }
    }

    private val _totalMatches = AtomicInteger(0)

    def totalMatches: Int = _totalMatches.get

    override def run(): Unit ={
        println("Matches consumer started...")
        while (true) {
            val recs = read()
            recs.foreach{ rec =>
                try{
                    _totalMatches.incrementAndGet()
                    println(s"Received match record: {$rec}, total matches: ${totalMatches}")
                }catch {
                    case e: Exception => println(s"Error parsing record: {$rec}, error: ${e.getMessage()}")
                }
            }
        }
    }
}

