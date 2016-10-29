package com.github.sorhus.webalytics.akka.model

import com.github.sorhus.webalytics.akka.segment.{Bitset, QuerySegmentState}
import org.roaringbitmap.RoaringBitmap

sealed trait Command
case class CloseBucket(b: Bucket) extends Command
case object Debug extends Command
case object GetAll extends Command
case class LoadImmutable(bucket: Bucket, space: Option[Element] = None) extends Command
case class MakeImmutable(bucket: Bucket, state: Map[Dimension, Map[Value, Bitset[RoaringBitmap]]] = Map.empty)
case class PostCommand(bucket: Bucket, elementId: ElementId, element: Element, persist: Boolean = true) extends Command
case object SaveSnapshot extends Command
case object Shutdown extends Command

case class QueryCommand(query: Query, space: Element, state: Option[QuerySegmentState] = None) extends Command
case class CountCommand(domain: Option[Element] = None, state: Option[QuerySegmentState] = None) extends Command


sealed trait AckOrNack {
  def *(that: AckOrNack): AckOrNack = Nack
}

case object Ack extends AckOrNack {
  override def *(that: AckOrNack) = if(that == Ack) Ack else Nack
}
case object Nack extends AckOrNack

