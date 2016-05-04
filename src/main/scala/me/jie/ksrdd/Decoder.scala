package me.jie.ksrdd

import kafka.serializer.Decoder
import kafka.utils.VerifiableProperties


/**
  * Created by jie on 4/29/16.
  */
class DefaultDecoder(props: VerifiableProperties = null) extends Decoder[Array[Byte]] with Serializable{
    def fromBytes(bytes: Array[Byte]): Array[Byte] = bytes
}

class StringDecoder(props: VerifiableProperties = null) extends Decoder[String] with Serializable {
  val encoding =
    if(props == null)
      "UTF8"
    else
      props.getString("serializer.encoding", "UTF8")

  def fromBytes(bytes: Array[Byte]): String = {
    new String(bytes, encoding)
  }
}