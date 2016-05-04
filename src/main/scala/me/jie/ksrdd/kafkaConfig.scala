package me.jie.ksrdd

import java.util.Properties

import kafka.utils.VerifiableProperties

/**
  * Created by jie on 4/28/16.
  */


class kafkaConfig private (props: VerifiableProperties) {
    import kafkaConfig._

    val metaDataBrokerList = props.getString("metadata.broker.list")
    val consumerId = props.getString("consumer.id", defaultConsumerId)
    val socketTimeoutMs = props.getInt("socket.timeout.ms", defaultSocketTimeoutMs)
    require(socketTimeoutMs > 0, "socket.timeout.ms must greater than 0 ")

    val socketReceiveBufferBytes = props.getInt("socket.receive.buffer.bytes", 10)

    val refreshLeaderBackoffMs = props.getInt("refresh.leader.backoff.ms", DefaultRefreshLeaderBackoffMs)
    require(refreshLeaderBackoffMs > 0, "refresh.leader.backoff.ms must be greater than 0")

    val retries = props.getInt("retries", DefaultRetries)
}
object kafkaConfig{

    def apply(props: Properties): kafkaConfig = new kafkaConfig(new VerifiableProperties(props))
    val defaultConsumerId = ""
    val defaultSocketTimeoutMs = 30 * 1000
    val DefaultRefreshLeaderBackoffMs = 200
    val DefaultRetries = 3
}