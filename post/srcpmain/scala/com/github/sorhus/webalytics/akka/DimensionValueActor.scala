package com.github.sorhus.webalytics.akka

import akka.actor.{ActorRef, Props}
import akka.persistence._
import com.github.sorhus.webalytics.model._
import org.slf4j.LoggerFactory

class DimensionValueActor(audienceActor: ActorRef) extends PersistentActor {

  val log = LoggerFactory.getLogger(getClass)

  var state = DimensionValues()

  def handle(e: PostMetaEvent) = {
    state = state.update(e)
  }

  override def persistenceId: String = "dimension-value-actor"

  override def receiveRecover: Receive = {

    case e: PostMetaEvent =>
      handle(e)
      log.info("received recover postmetaevent {}", e)

    case SnapshotOffer(_, snapshot: DimensionValues) =>
      log.info("restoring state from snapshot")
      state = snapshot

    case x =>
      log.info("received recover {}", x)

  }

  override def receiveCommand: Receive = {

    case e: PostMetaEvent =>
      log.info("received postmetaevent {}", e)
      persist(e)(handle)

    case query: Query =>
      val space = state.get(query.dimensions)
      audienceActor forward QueryEvent(query, space)

    case Getall =>
      sender() ! state.getAll

    case SaveSnapshot =>
      log.info("saving snapshot: {}", state)
      saveSnapshot(state)

    case Shutdown =>
      sender() ! context.stop(self)

    case Debug =>
      state.debug()

    case SaveSnapshotSuccess(metadata) =>
      log.info(s"snapshot saved. seqNum:${metadata.sequenceNr}, timeStamp:${metadata.timestamp}")

    case SaveSnapshotFailure(_, reason) =>
      log.info("failed to save snapshot", reason)

    case x =>
      println(s"dimval recieved $x")

  }

}

object DimensionValueActor {
  def props(audienceDao: ActorRef): Props = Props(new DimensionValueActor(audienceDao))
}

case class DimensionValues(elems: Map[Bucket, Element] = Map[Bucket, Element]()) extends Serializable {
  def update(event: PostMetaEvent): DimensionValues = copy(elems + (event.bucket -> event.element))
  def get(dimensions: List[Dimension]): Element = {
    dimensions match {
      case Dimension("*") :: Nil => getAll
      case _ => Element(getAll.e.filter{case(d,v) => dimensions.contains(d)})
    }
  }
  def getAll: Element = Element.merge {
    elems.map{case(bucket, elements) => elements}.toList
  }
  def debug(): Unit = {
    println(elems)
  }
}
