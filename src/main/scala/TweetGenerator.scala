import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.annotation.tailrec
import scala.io.Source
import scala.util.Random

object TweetGenerator extends App {

  def repeat[A](n: Int)(f: => A): List[A] = (1 to n).map(_ => f).toList

  @tailrec
  def sendMessages(words: Vector[String], producer: KafkaProducer[String, String])(n: Int): Unit = {
    if (n == 0) producer.close()
    else {
      val sentence = Random.shuffle(repeat(Random.nextInt(10))(Random.nextInt(words.length)).map(words))
      val record = new ProducerRecord[String, String](topic, "key", sentence.mkString(" "))
      producer.send(record)
      sendMessages(words, producer)(n - 1)
    }
  }

  val kafkahost: String = args(0)
  val topic: String = args(1)
  val amount: Int = args(2).toInt

  val props = new Properties()
  props.put("bootstrap.servers", kafkahost)

  val producer = new KafkaProducer[String, String](props)
  val words = Source.fromResource("words.txt").getLines().toVector

  sendMessages(words, producer)(amount)
}
