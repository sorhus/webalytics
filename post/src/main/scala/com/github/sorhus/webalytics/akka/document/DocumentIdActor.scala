package com.github.sorhus.webalytics.akka.document

import akka.actor.{ActorRef, Props}
import akka.persistence._
import com.github.sorhus.webalytics.akka.model._
import org.slf4j.LoggerFactory

class DocumentIdActor(audienceActor: ActorRef, queryActor: ActorRef, id: Int, n: Int) extends PersistentActor {

  val log = LoggerFactory.getLogger(getClass)

  var state = DocumentIdState(n, counter = id)

  override def persistenceId: String = s"document-id-actor-$id"

  override def receiveRecover: Receive = {

    case e: PostEvent =>
      log.debug("received recover postevent")
      state = state.update(e.elementId, e.documentId)
      post(e)

    case SnapshotOffer(_, snapshot: DocumentIdState) =>
      log.info("restoring state from snapshot")
      state = snapshot

    case x =>
      log.info("received recover {}", x)

  }

  def post(event: PostEvent): Unit = {
    audienceActor ! event
    queryActor ! PostMetaEvent(event.bucket, event.element)
  }

  def notifyAndPost(event: PostEvent): Unit = {
    sender() ! Ack
    post(event)
  }

  override def receiveCommand: Receive = {

    case e: PostCommand =>
      log.debug("received postevent")
      state = state.update(e.elementId)
      val documentId = state.get(e.elementId)
      val postEvent = PostEvent(e.bucket, e.elementId, documentId, e.element)
      persistAsync(postEvent)(notifyAndPost)

    case SaveSnapshot =>
      log.info("saving snapshot")
      saveSnapshot(state)

    case Shutdown =>
      sender() ! context.stop(self)

    case SaveSnapshotSuccess(metadata) =>
      log.info(s"snapshot saved. seqNum:${metadata.sequenceNr}, timeStamp:${metadata.timestamp}")
      deleteMessages(metadata.sequenceNr)

    case SaveSnapshotFailure(_, reason) =>
      log.info("failed to save snapshot: {}", reason)

    case DeleteMessagesSuccess(toSequenceNr) =>
      log.info(s"message deleted. sequNum {}", toSequenceNr)

    case DeleteMessagesFailure(reason, toSequenceNr) =>
      log.info(s"failed to delete message to sequenceNr: {} {}", toSequenceNr, reason)

    case x =>
      log.info(s"doc recieved {}", x)

  }

  override def recovery = {
    log.info("recovery")
    Recovery(fromSnapshot = SnapshotSelectionCriteria.Latest)
  }

}

object DocumentIdActor {
  def props(audienceActor: ActorRef, queryActor: ActorRef, id: Int = 0, n: Int = 1): Props =
    Props(new DocumentIdActor(audienceActor, queryActor, id, n))
}


