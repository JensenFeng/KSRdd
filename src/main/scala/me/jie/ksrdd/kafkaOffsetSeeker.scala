package me.jie.ksrdd

import java.util.Properties

import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo, PartitionOffsetsResponse}
import kafka.common.{ErrorMapping, TopicAndPartition}

import scala.annotation.tailrec

/**
  * Created by jie on 5/3/16.
  */
class kafkaOffsetSeeker(kafkaProps: Properties) {
  private val config = kafkaConfig(kafkaProps)

  private val kafkaHelper = new kafkaHelper(config)
  import kafkaHelper.{findLeader, buildConsumer}

  private val earliest = -2
  private val latest = -1

  def possibleOffsetBefore(topicAndPartition: TopicAndPartition, timeMillis: Long): Option[Long] = {
    val requestInfo = Map(topicAndPartition -> PartitionOffsetRequestInfo(timeMillis, 1))
    val request = OffsetRequest(requestInfo = requestInfo)

    @tailrec
    def fetchWithRetry(retries: Int): Option[Long] = {
      val leader = buildConsumer(findLeader(topicAndPartition))
      val response = leader.getOffsetsBefore(request)
      val PartitionOffsetsResponse(error, offsets) = response.partitionErrorAndOffsets(topicAndPartition)
      leader.close()

      (error, retries) match {
        case (ErrorMapping.NoError, _) => offsets.headOption
        case (_, config.retries) => throw ErrorMapping.exceptionFor(error)
        case (_, _) => Thread.sleep(config.refreshLeaderBackoffMs); fetchWithRetry(retries + 1)
      }
    }

    fetchWithRetry(0)
  }

  def earliestOffset(topicAndPartition: TopicAndPartition): Option[Long] =
    possibleOffsetBefore(topicAndPartition, earliest)

  def latestOffset(topicAndPartition: TopicAndPartition): Option[Long] =
    possibleOffsetBefore(topicAndPartition, latest)
}
