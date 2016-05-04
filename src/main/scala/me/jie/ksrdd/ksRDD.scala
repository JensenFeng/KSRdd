package me.jie.ksrdd

import java.util.Properties

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.{Decoder}
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

/**
  * Created by jie on 4/29/16.
  */
object ksRDD {
    private val log = LoggerFactory.getLogger(getClass)

    def apply[K, V](
             sc: SparkContext, kafkaProps: Properties,
             fetchInfo: Map[TopicAndPartition, Seq[OffsetFetchInfo]],
             fetchMessageMaxCount: Int = 1024 * 1024 * 1024,
             keyDecoder: Decoder[K] ,//= new DefaultDecoder ,
             valueDecoder: Decoder[V] //= new DefaultDecoder
           ) =
    new ksRDD(sc, kafkaProps, fetchInfo, fetchMessageMaxCount, keyDecoder, valueDecoder)
}

class ksRDD[K, V] private(
                           _sc: SparkContext, kafkaProps: Properties,
                           fetchInfo: Map[TopicAndPartition, Seq[OffsetFetchInfo]],
                           fetchMessageMaxCount: Int,
                           keyDecoder: Decoder[K], valueDecoder: Decoder[V]
                         ) extends RDD[MessageAndMetadata[K,V]](_sc, Nil){
  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[MessageAndMetadata[K, V]] = {
      if(context.attemptNumber() > 1){
          log.warn(s"Attempt ${context.attemptNumber} times for fetching ${split}")
      }

      val taskStartTime = System.currentTimeMillis()
      context.addTaskCompletionListener(_ => {
          val used = (System.currentTimeMillis() - taskStartTime) / 1000.0
          if(used > 300.0) {
            log.warn(s"Fetched ${split} in a quite Long time (${used}s)")
          }
          })
      val topicAndPartition = split.asInstanceOf[kafkaRDDPartition].topicAndPartition
      val offsetFetchInfo = split.asInstanceOf[kafkaRDDPartition].offsetFetchInfo
      kafkaStream(kafkaProps, keyDecoder, valueDecoder).fetch(topicAndPartition, offsetFetchInfo).iterator
  }

  override protected def getPartitions: Array[Partition] = {
      def slice(offsetFetchInfo: OffsetFetchInfo, maxSizeForSlice: Int): Seq[OffsetFetchInfo] = {
          val OffsetFetchInfo(from, to) = offsetFetchInfo
          (1 + to - from).ensuring(_ < Int.MaxValue).toInt match {
            case totalSize if totalSize > maxSizeForSlice => {
                val buckets = (totalSize + maxSizeForSlice - 1) / maxSizeForSlice
                val (size, rest) = (totalSize / buckets, totalSize % buckets)
                val sliceSizes = (1 to buckets) map ( x => if(x <= rest) 1 else 0 ) map(_ + size)
                val grads = sliceSizes.inits.map(_.sum).map(_ + from - 1).toSeq.reverse
                grads.sliding(2).map(slice => OffsetFetchInfo(slice(0) + 1, slice(1))).toSeq
            }
            case _ => Seq(offsetFetchInfo)
        }
      }
      fetchInfo.map {
        case (topicAndPartition, partitionFetchInfos) =>
          partitionFetchInfos.flatMap(slice(_, fetchMessageMaxCount)).map { (topicAndPartition, _)}
      }.flatten.zipWithIndex.map {
        case ((topicAndPartition, offsetFetchInfo), index) => new kafkaRDDPartition(id, index,
                                                                topicAndPartition, offsetFetchInfo)
      }.toArray
  }
}


private[ksrdd] class kafkaRDDPartition(
                                 rddId: Int,
                                 override val index: Int,
                                 val topicAndPartition: TopicAndPartition,
                                 val offsetFetchInfo: OffsetFetchInfo
                                 ) extends Partition {
    override def hashCode: Int = 41 * (41 + rddId) + index

    override def toString: String = "{rddId: %d, index: %d, topic: %s, partition: %d, offset: %s}"
          .format(rddId, index, topicAndPartition.topic, topicAndPartition.partition, offsetFetchInfo)
}