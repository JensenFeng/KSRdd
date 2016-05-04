package me.jie.ksrdd

import java.util.Properties

import kafka.api.{FetchRequest, FetchResponsePartitionData, PartitionFetchInfo}
import kafka.common.{ErrorMapping, TopicAndPartition}
import kafka.consumer.SimpleConsumer
import kafka.message.{MessageAndMetadata, MessageAndOffset}
import kafka.serializer.Decoder
import org.slf4j.LoggerFactory

/**
  * Created by jie on 4/27/16.
  */
object kafkaStream {
    private val log = LoggerFactory.getLogger(getClass)

    def apply(kafkaProps: Properties): kafkaStream[Array[Byte], Array[Byte]] =
      apply(kafkaProps, new DefaultDecoder, new DefaultDecoder)
    def apply[K, V](kafkaProps: Properties, keyDecoder: Decoder[K], valueDecoder: Decoder[V]): kafkaStream[K, V] =
      new kafkaStream(kafkaConfig(kafkaProps), keyDecoder, valueDecoder)

}

case class OffsetFetchInfo(from: Long, to: Long) {
    require(0 <= from && from <= to)

    override def toString = s"Offset[${from}, ${to}]"
}

class kafkaStream[K, V] private (config: kafkaConfig, keyDecoder: Decoder[K], valueDecoder: Decoder[V]) {

  import kafkaStream._

  private val fetchMessageMaxBytes = config.fetchMessageMaxBytes
  //config
  private val retries = config.retries
  //config
  private val refreshLeaderBackoffMs = config.refreshLeaderBackoffMs

  private val kafkaHelper = new kafkaHelper(config)
  import kafkaHelper.{buildConsumer, findLeader}

  //config
  def fetch(topicAndPartition: TopicAndPartition, offsetFetchInfo: OffsetFetchInfo)
  : Stream[MessageAndMetadata[K, V]] = {
    val OffsetFetchInfo(offsetFrom, offsetTo) = offsetFetchInfo

    def buildMessageAndMetadata(messageAndOffset: MessageAndOffset): MessageAndMetadata[K, V]
    = MessageAndMetadata(topicAndPartition.topic, topicAndPartition.partition,
      messageAndOffset.message, messageAndOffset.offset,
      keyDecoder, valueDecoder)

    def doFetch(consumer: SimpleConsumer, offset: Long, retriesLeft: Int): Stream[Array[MessageAndMetadata[K, V]]] = {
      val FetchResponsePartitionData(errorCode, _, messageSet) = consumer.fetch(FetchRequest(
        requestInfo = Map(topicAndPartition -> PartitionFetchInfo(offset, fetchMessageMaxBytes))
      )).data(topicAndPartition)

      (errorCode, retriesLeft) match {
        case (ErrorMapping.NoError, _) => {
          val messageAndOffsets = messageSet.toArray
          messageAndOffsets.length match {
            // message is empty
            case 0 => {
              consumer.close()
              if (offset >= offsetTo) {
                Stream.empty
              } else {
                log.error(s"error fetch offset ${offset} at ${consumer.host}:${consumer.port}")
                throw ErrorMapping.exceptionFor(ErrorMapping.OffsetOutOfRangeCode)
              }
            }
            case _ => {
              val lastMessageAndOffset = messageAndOffsets.last
              if (lastMessageAndOffset.offset >= offsetTo) {
                consumer.close()
                messageAndOffsets.filter(_.offset <= offsetTo).map(buildMessageAndMetadata) #:: Stream.empty
              } else {
                messageAndOffsets.map(buildMessageAndMetadata) #:: doFetch(consumer, lastMessageAndOffset.nextOffset,
                  retries)
              }
            }
          }
        }
        case (_, 0) => {
          consumer.close()
          log.error(s"error fetch offset ${offset} at ${consumer.host}:${consumer.port}")
          throw ErrorMapping.exceptionFor(errorCode)
        }
        case (_, _) => {
          consumer.close()
          Thread.sleep(refreshLeaderBackoffMs)
          doFetch(buildConsumer(findLeader(topicAndPartition)), offset, retriesLeft - 1)
        }
      }
    }
    doFetch(buildConsumer(findLeader(topicAndPartition)), offsetFrom, retries).flatten.dropWhile(_.offset < offsetFrom)
  }
}