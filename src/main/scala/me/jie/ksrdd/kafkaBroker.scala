package me.jie.ksrdd

/**
  * Created by jie on 4/28/16.
  */

case class kafkaBroker(host: String, port: Int)

object kafkaBroker {
  def apply(addr: String): kafkaBroker = addr.split(":") match {
    case Array(host, port) => kafkaBroker(host, port.toInt)
    case Array(host) => kafkaBroker(host, 9292)
    case _ => throw new IllegalArgumentException(addr)
  }
}
