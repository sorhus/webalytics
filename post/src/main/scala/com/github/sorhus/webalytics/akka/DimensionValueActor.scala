package com.github.sorhus.webalytics.akka

import akka.{Done, NotUsed}
import akka.actor.Actor.Receive
import akka.actor.{Actor, ActorRef, Props}
import akka.persistence._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import com.github.sorhus.webalytics.model._
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class DimensionValueActor(audienceActor: ActorRef) extends PersistentActor {

  val log = LoggerFactory.getLogger(getClass)
  log.info(s"$getClass alive")

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
      log.info(s"dimval recieved $x")

  }

}

object DimensionValueActor {
  def props(audienceDao: ActorRef): Props = Props(new DimensionValueActor(audienceDao))
  val persistenceId: String = "dimension-value-actor"
}

case class DimensionValues(elems: Map[Bucket, Element] = Map[Bucket, Element]()) extends Serializable {
  def update(event: PostMetaEvent): DimensionValues = {
    val updated = event.bucket -> (elems.getOrElse(event.bucket, Element()) + event.element)
    copy(elems = elems + updated)
  }
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

