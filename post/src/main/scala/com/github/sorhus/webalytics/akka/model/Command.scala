package com.github.sorhus.webalytics.akka.model

import com.github.sorhus.webalytics.akka.segment.MutableMapWrapper

sealed trait Command
case class CloseBucket(b: Bucket) extends Command
case object Debug extends Command
case object GetAll extends Command
case class LoadImmutable(bucket: Bucket, space: Option[Element] = None) extends Command
case class MakeImmutable(bucket: Bucket, state: Option[MutableMapWrapper.type] = None)
case class PostCommand(bucket: Bucket, elementId: ElementId, element: Element) extends Command
case object SaveSnapshot extends Command
case object Shutdown extends Command

sealed trait AckOrNack
case object Ack extends AckOrNack
case object Nack extends AckOrNack

