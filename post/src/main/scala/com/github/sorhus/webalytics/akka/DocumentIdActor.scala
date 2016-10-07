package com.github.sorhus.webalytics.akka

import akka.actor.{ActorRef, Props}
import akka.persistence._
import com.github.sorhus.webalytics.model._
import org.slf4j.LoggerFactory

class DocumentIdActor(audienceActor: ActorRef, queryActor: ActorRef) extends PersistentActor {

  val log = LoggerFactory.getLogger(getClass)
  log.info(s"$getClass alive")

  var state = DocumentIds()

  override def persistenceId: String = "document-id-actor"

  override def receiveRecover: Receive = {

    case e: PostEvent1 =>
      log.info("received recover postevent {}", e)
      state = state.update(e.elementId)

    case SnapshotOffer(_, snapshot: DocumentIds) =>
      log.info("restoring state from snapshot")
      state = snapshot

    case x =>
      log.info("received recover {}", x)

  }

  def handle(event: PostEvent1): Unit = {
    log.info("handling event")
    state = state.update(event.elementId)
    val documentId = state.get(event.elementId)
    queryActor ! PostMetaEvent(event.bucket, event.element)
    audienceActor forward PostEvent(event.bucket, documentId, event.element)
  }

  override def receiveCommand: Receive = {

    case e: PostEvent1 =>
      log.info("received postevent {}", e)
      persist(e)(handle)

    case SaveSnapshot =>
      log.info("saving snapshot")
      saveSnapshot(state)

    case Shutdown =>
      sender() ! context.stop(self)

    case SaveSnapshotSuccess(metadata) =>
      log.info(s"snapshot saved. seqNum:${metadata.sequenceNr}, timeStamp:${metadata.timestamp}")

    case SaveSnapshotFailure(_, reason) =>
      log.info("failed to save snapshot: {}", reason)

    case x =>
      log.info(s"doc recieved {}", x)

  }

  override def recovery = {
    log.info("recovery")
    Recovery(fromSnapshot = SnapshotSelectionCriteria.Latest)
  }

}

object DocumentIdActor {
  def props(audienceActor: ActorRef, queryActor: ActorRef): Props = Props(new DocumentIdActor(audienceActor, queryActor))
}

case class DocumentIds(counter: Long = 0, ids: Map[ElementId, DocumentId] = Map.empty) extends Serializable {
  def get(elementId: ElementId): DocumentId = ids(elementId)
  def update(elementId: ElementId): DocumentIds = {
    if(ids.contains(elementId))
      this
    else
      copy(counter + 1, ids + (elementId -> DocumentId(counter + 1)))
  }
}
