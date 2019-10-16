import java.util.Properties

import javax.sound.sampled.AudioFormat.Encoding
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.annotation.tailrec
import scala.io.{Codec, Source}

object TweetGenerator extends App {

  def timed(f: => Any): Long = {
    val start = System.currentTimeMillis()
    f
    System.currentTimeMillis() - start
  }

  @tailrec
  def sendMessages(sentences: List[String], producer: KafkaProducer[String, String])
                  (n: Int, sleep: Int, dryRun: Boolean = false): Unit = {
    if (n == 0 || sentences.isEmpty) {
      producer.close()
    } else {
      val sentence :: rest = sentences
      val record = new ProducerRecord[String, String](topic, "key", sentence)

      if (dryRun) println(sentence)
      else producer.send(record)

      Thread.sleep(sleep - 1)
      sendMessages(rest, producer)(n - 1, sleep, dryRun)
    }
  }

  // kafka host:port, topic, amount of sentences to generate, time to sleep between messages in milliseconds
  val host :: topic :: amount :: sleep :: Nil = args.toList

  val props = new Properties()
  props.put("bootstrap.servers", host)
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)
  val sentences = Source
    .fromResource("positive.txt")(Codec.ISO8859)
    .getLines()
    .map{ line =>
      line
        .replaceAll("""[?.!,":();]|""", "")
        .replaceAll("""\s\s+""", " ")
    }
    .toList

  val elapsedTime = timed(sendMessages(sentences, producer)(amount.toInt, sleep.toInt))

  println(s"finished in $elapsedTime. Average time per sentence: ${elapsedTime / amount.toInt}")
}
