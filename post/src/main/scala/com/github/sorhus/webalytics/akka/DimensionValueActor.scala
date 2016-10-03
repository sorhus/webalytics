package com.github.sorhus.webalytics.akka

import akka.actor.{ActorRef, Props}
import akka.persistence.{PersistentActor, Recovery, SnapshotOffer}
import com.github.sorhus.webalytics.model._
import org.slf4j.LoggerFactory

import scala.collection.mutable

class DimensionValueActor(audienceActor: ActorRef) extends PersistentActor {

  val log = LoggerFactory.getLogger(getClass)

  val state = new DimensionValues

  override def persistenceId: String = "dimension-value-actor"

  override def receiveRecover: Receive = {
    case e: PostMetaEvent => state.add(e)
    case SnapshotOffer(_, snapshot: DimensionValues) => snapshot.elems.foreach{case(k,v) => state.elems.put(k,v)}
  }

  override def receiveCommand: Receive = {
    case e: PostMetaEvent =>
      log.info("received postmetaevent {}", e)
      persist(e)(state.add)
    case query: Query =>
      val space = state.get(query.dimensions)
      audienceActor forward QueryEvent(query, space)
    case Getall =>
//      println("received \"getall\" message")
      sender() ! state.getAll
    case Shutdown => sender() ! context.stop(self)
    case Debug => state.debug()
    case x => println(s"dimval recieved $x")

  }

}

object DimensionValueActor {
  def props(audienceDao: ActorRef): Props = Props(new DimensionValueActor(audienceDao))
}

class DimensionValues {
  val elems = mutable.Map[Bucket, Element]()
  def add(event: PostMetaEvent): Unit = add(event.bucket, event.element)
  def add(bucket: Bucket, element: Element): Unit = {
    elems.put(bucket, Element.merge(elems.getOrElse(bucket, Element(Map())) :: element :: Nil))
  }
  def get(dimensions: List[Dimension]): Element = {
//    println(getAll())
//    println(dimensions)
    dimensions match {
      case Dimension("*") :: Nil => getAll()
      case _ => Element(getAll().e.filter{case(d,v) => dimensions.contains(d)})
    }
  }
  def getAll(): Element = {
    val all = elems.map{case(bucket, elements) => elements}.toList
    Element.merge(all)
  }
  def debug(): Unit = {
    println(elems)
  }
}
