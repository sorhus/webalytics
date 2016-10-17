package com.github.sorhus.webalytics.akka.segment

import akka.actor.{ActorRef, Props}
import akka.persistence._
import com.github.sorhus.webalytics.akka.model._
import org.slf4j.LoggerFactory

class SegmentActor(immutableSegmentActor: ActorRef) extends PersistentActor {

  val log = LoggerFactory.getLogger(getClass)

  var state = new MutableSegmentState

  override def persistenceId: String = "segment-actor"

  def handle(e: PostEvent): Unit = {
    state.post(e)
  }

  override def receiveCommand: Receive = {

    case e: PostEvent =>
      log.debug("received postevent")
//      persistAsync(e)(handle)
      handle(e)

    case q: QueryEvent =>
      immutableSegmentActor forward q.copy(state = Some(QuerySegmentState(state)))

    case CloseBucket(bucket) =>
      log.info("closing bucket")
      state remove bucket
      log.info("saving snapshot")
      saveSnapshot(state)
      sender() ! Ack

    case SaveSnapshot =>
      log.info("saving snapshot")
      saveSnapshot(state)

    case cmd @ MakeImmutable(bucket, _) =>
      immutableSegmentActor forward cmd.copy(state = state.getCopy(bucket))

    case Shutdown =>
      context.stop(self)
      sender() ! Ack

//    case Debug => state.debug()

    case SaveSnapshotSuccess(metadata) =>
      log.info(s"snapshot saved. seqNum:{}, timestamp: {}", metadata.sequenceNr, metadata.timestamp)
      deleteMessages(metadata.sequenceNr)

    case SaveSnapshotFailure(_, reason) =>
      log.info("failed to save snapshot: {}", reason)

    case DeleteMessagesSuccess(toSequenceNr) =>
      log.info(s"message deleted. sequNum {}", toSequenceNr)

    case DeleteMessagesFailure(reason, toSequenceNr) =>
      log.info(s"failed to delete message to sequenceNr: {} {}", toSequenceNr, reason)

    case x =>
      log.info(s"audience recieved {}", x)

  }

  override def receiveRecover: Receive = {

    case e: PostEvent =>
      log.debug("received recover postevent")
      state.post(e)

    case SnapshotOffer(_, snapshot: MutableSegmentState) =>
      log.info("received recover snapshot")
      state = snapshot

    case x =>
      log.info("received recover {}", x)
  }

}

object SegmentActor {
  def props(immutableActor: ActorRef): Props = Props(new SegmentActor(immutableActor))
}



