import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.annotation.tailrec
import scala.io.Source
import scala.util.Random

object TweetGenerator extends App {

  def repeat[A](n: Int)(f: => A): List[A] = (1 to n).map(_ => f).toList
  def timed(f: => Any): Long = {
    val start = System.currentTimeMillis()
    f
    System.currentTimeMillis() - start
  }

  @tailrec
  def sendMessages(words: Vector[String], producer: KafkaProducer[String, String])
                  (n: Int, sleep: Int, dryRun: Boolean = false): Unit = {
    if (n == 0) {
      producer.close()
    } else {
      val sentence = Random.shuffle(repeat(Random.nextInt(21) + 1)(Random.nextInt(words.length)).map(words))
      val record = new ProducerRecord[String, String](topic, "key", sentence.mkString(" "))

      if (dryRun) println(sentence.mkString(" "))
      else producer.send(record)

      Thread.sleep(sleep - 1)
      sendMessages(words, producer)(n - 1, sleep, dryRun)
    }
  }

  // kafka host:port, topic, amount of sentences to generate, time to sleep between messages in milliseconds
  val host :: topic :: amount :: sleep :: Nil = args.toList

  val props = new Properties()
  props.put("bootstrap.servers", host)
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)
  val words = Source.fromResource("words.txt").getLines().toVector

  val elapsedTime = timed(sendMessages(words, producer)(amount.toInt, sleep.toInt))

  println(s"finished in $elapsedTime. Average time per sentence: ${elapsedTime / amount.toInt}")
}
