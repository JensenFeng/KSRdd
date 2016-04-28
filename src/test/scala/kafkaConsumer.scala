import java.util
import java.util.Properties

import kafka.consumer.ConsumerConfig

/**
  * Created by jie on 4/28/16.
  */
object kafkaConsumer {
    def apply(topic: String): kafkaConsumer = new kafkaConsumer(topic)
}

class kafkaConsumer private (topic: String) {
    val consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig)

    def createConsumerConfig: ConsumerConfig = {
        val props: Properties = new Properties()
        props.put("zookeeper.connect", "")
        props.put("group.id", "")
        props.put("zookeeper.session.timeout.ms", "40000")
        props.put("zookeeper.sync.time.ms", "200")
        props.put("auto.commit.interval.ms", "1000")
        new ConsumerConfig(props)
    }

    def start = {
      val topicsCountMap = new util.HashMap[String,Integer]()
      topicsCountMap.put(topic, new Integer(1))
      val consumerMap = consumer.createMessageStreams(topicsCountMap)
      consumerMap.get(topic).get(0).foreach(println)
    }
}

object Main {
   def main(args: Array[String]): Unit = {
      val t = kafkaConsumer
      t("topic").start
   }
}