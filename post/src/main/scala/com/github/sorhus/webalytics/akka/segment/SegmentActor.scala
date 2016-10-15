package com.github.sorhus.webalytics.akka.segment

import akka.actor.{ActorRef, Props}
import akka.persistence._
import com.github.sorhus.webalytics.akka.model._
import org.slf4j.LoggerFactory

class SegmentActor(immutableActor: ActorRef = null) extends PersistentActor {

  val log = LoggerFactory.getLogger(getClass)

  var state = new MutableSegmentState

  override def persistenceId: String = "bitset-audience-actor"

  override def receiveRecover: Receive = {

    case e: PostEvent =>
      log.debug("received recover postevent")
      state.post(e)

    case SnapshotOffer(_, snapshot: MutableSegmentState) =>
      log.info("received recover snapshot {}", snapshot)
      state = snapshot

    case x =>
      log.info("received recover {}", x)
  }

  def handle(e: PostEvent): Unit = {
//    sender() ! Ack
    state.post(e)
  }

  override def receiveCommand: Receive = {

    case e: PostEvent =>
      log.debug("received postevent")
//      persist(e)(handle)
//      persistAsync(e)(handle)
      handle(e)

    case QueryEvent(query: Query, space: Element) =>
      log.debug("received query and space {}", (query, space))
      val response: Map[String, Map[String, Map[String, Long]]] = state.getCount(query, space.e)
        .map{case(bucket, dimensions) =>
          bucket.b -> dimensions.map{case(dimension, values) =>
          dimension.d -> values.map{case(value, count) =>
            value.v -> count
          }.toMap
        }.toMap
      }.toMap
      sender() ! response

    case CloseBucket(bucket) =>
      log.info("closing bucket")
      state remove bucket
      sender() ! Ack

    case SaveSnapshot =>
      // This should only be called after the "child" actors have been snapshot
      log.info("saving snapshot")
      saveSnapshot(state)

    case cmd @ MakeImmutable(bucket, _) =>
//      immutableActor forward cmd.copy(bitsets = state.bitsets(bucket))
      // TODO don't send the entire thing
      immutableActor forward cmd.copy(state = Some(state.bitsets))

    case Shutdown => sender() ! context.stop(self)

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

}

object SegmentActor {
  def props(immutableActor: ActorRef): Props = Props(new SegmentActor(immutableActor))
  def props(): Props = Props(new SegmentActor())
}


