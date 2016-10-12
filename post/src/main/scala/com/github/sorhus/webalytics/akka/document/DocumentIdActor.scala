package com.github.sorhus.webalytics.akka.document

import akka.actor.{ActorRef, Props}
import akka.persistence._
import com.github.sorhus.webalytics.model._
import org.slf4j.LoggerFactory

class DocumentIdActor(audienceActor: ActorRef, queryActor: ActorRef, id: Int, n: Int) extends PersistentActor {

  val log = LoggerFactory.getLogger(getClass)
  log.info(s"$getClass alive")

  var state = DocumentIds(n, counter = id)

  override def persistenceId: String = s"document-id-actor-$id"

  override def receiveRecover: Receive = {

    case e: PostEvent =>
//      log.info("received recover postevent {}", e)
      log.info("received recover postevent")
      state = state.update(e.elementId, e.documentId)
      post(e)

    case SnapshotOffer(_, snapshot: DocumentIds) =>
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
//    log.info("handling event")
//    audienceActor forward event
    sender() ! Ack
    post(event)
  }

  override def receiveCommand: Receive = {

    case e: PostCommand =>
//      log.info("received postevent {}", e)
      log.info("received postevent")
      state = state.update(e.elementId)
      val documentId = state.get(e.elementId)
//      persist(e)(handle)
      val postEvent = PostEvent(e.bucket, e.elementId, documentId, e.element)
      persistAsync(postEvent)(notifyAndPost)

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
  def props(audienceActor: ActorRef, queryActor: ActorRef, id: Int = 0, n: Int = 1): Props =
    Props(new DocumentIdActor(audienceActor, queryActor, id, n))
}


