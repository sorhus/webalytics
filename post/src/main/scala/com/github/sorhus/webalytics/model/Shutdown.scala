package com.github.sorhus.webalytics.model

case object Shutdown
case object Getall
case object Debug
case object SaveSnapshot
case class Immutate(bucket: Bucket)
case class Initialize(bucket: Bucket, space: Option[Element] = None)
case class Load(file: String, bucket: Bucket)
sealed trait AckOrNack
case object Ack extends AckOrNack
case object Nack extends AckOrNack