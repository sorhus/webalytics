package com.github.sorhus.webalytics.akka

import akka.actor.{ActorRef, Props}
import akka.persistence.{PersistentActor, SnapshotOffer}
import com.github.sorhus.webalytics.model._
import org.slf4j.LoggerFactory

import scala.collection.mutable

class DocumentIdActor(audienceActor: ActorRef, queryActor: ActorRef) extends PersistentActor {

  val log = LoggerFactory.getLogger(getClass)

  val state = new DocumentIds

  override def persistenceId: String = "document-id-actor"

  def handle(event: PostEvent1): Unit = {
    val documentId = state.getDocumentId(event.elementId)
    queryActor ! PostMetaEvent(event.bucket, event.element)
    audienceActor forward PostEvent(event.bucket, documentId, event.element)
  }

  override def receiveCommand: Receive = {
    case e: PostEvent1 =>
      log.info("received event {}", e)
      persist(e)(handle)
    case Shutdown => sender() ! context.stop(self)
    case x => println(s"doc recieved $x")
  }

  override def receiveRecover: Receive = {
    case e: PostEvent1 => state.getDocumentId(e.elementId)
    case SnapshotOffer(_, snapshot: DocumentIds) =>
      snapshot.ids.foreach{case(k,v) => state.ids.put(k,v)}
      state.counter = snapshot.counter
  }

}

object DocumentIdActor {
  def props(audienceActor: ActorRef, queryActor: ActorRef): Props = Props(new DocumentIdActor(audienceActor, queryActor))
}

class DocumentIds {
  val ids = mutable.Map[String, DocumentId]()
  var counter = 0
  def getDocumentId(elementId: ElementId): DocumentId = ids.getOrElse(elementId.e, {
    counter +=1
    val id = DocumentId(counter)
    ids.put(elementId.e, id)
    id
  })
}
