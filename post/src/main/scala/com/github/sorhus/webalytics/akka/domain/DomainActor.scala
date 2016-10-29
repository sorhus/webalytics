package com.github.sorhus.webalytics.akka.domain

import akka.actor.{ActorRef, Props}
import akka.persistence._
import com.github.sorhus.webalytics.akka.model._

class DomainActor(segmentActor: ActorRef, immutableSegmentActor: ActorRef) extends TDomainActor {

  override def receiveCommand: Receive = {

    case e: PostMetaEvent =>
      log.debug("received postmetaevent")
//      persistAsync(e)(handle)
      handle(e)

    case query: Query =>
      log.debug("received query {}", query)
      val space = state.get(query.dimensions)
      log.debug("space is {}", space)
      segmentActor forward QueryCommand(query, space)

    case i: LoadImmutable =>
      log.info("forwarding LoadImmutable")
      immutableSegmentActor forward i.copy(space = Some(state.get(i.bucket)))

    case GetAll =>
      sender() ! state.getAll

    case c: CountCommand =>
      segmentActor forward c.copy(domain = Some(state.getAll))

    case SaveSnapshot =>
      log.info("saving snapshot")
      saveSnapshot(state)

    case Shutdown =>
      context.stop(self)
      sender() ! Ack

    case Debug =>
      state.debug()

    case SaveSnapshotSuccess(metadata) =>
      log.info(s"snapshot saved. seqNum:${metadata.sequenceNr}, timeStamp:${metadata.timestamp}")
      deleteMessages(metadata.sequenceNr)

    case SaveSnapshotFailure(_, reason) =>
      log.warn("failed to save snapshot", reason)

    case DeleteMessagesSuccess(toSequenceNr) =>
      log.info(s"message deleted. seqNum {}", toSequenceNr)

    case DeleteMessagesFailure(reason, toSequenceNr) =>
      log.warn(s"failed to delete message to sequenceNr: {} {}", toSequenceNr, reason)

    case x =>
      log.info(s"received $x")

  }

  override def receiveRecover: Receive = {

    case e: PostMetaEvent =>
      log.debug("received recover postmetaevent")
      handle(e)

//    case SnapshotOffer(_, snapshot: DomainState) =>
    case SnapshotOffer(_, snapshot: MutableDomainState) =>
      log.info("restoring state from snapshot")
      state = snapshot

    case x =>
      log.info("received recover {}", x)

  }

}

object DomainActor {
  def props(segmentActor: ActorRef, immutableSegmentActor: ActorRef): Props = {
    Props(new DomainActor(segmentActor, immutableSegmentActor: ActorRef))
  }
}
