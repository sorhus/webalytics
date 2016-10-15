package com.github.sorhus.webalytics.akka.domain

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, Props}
import akka.persistence._
import akka.pattern.ask
import akka.util.Timeout
import com.github.sorhus.webalytics.akka.model._

import scala.collection.immutable.Iterable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

class DomainActor(segmentActor: ActorRef, immutableSegmentActor: ActorRef, readOnlyMetaActor: Option[ActorRef]) extends TDomainActor {

  implicit val timeout = Timeout(10, TimeUnit.SECONDS)
  import concurrent.ExecutionContext.Implicits.global

  override def receiveCommand: Receive = {

    case e: PostMetaEvent =>
      log.debug("received postmetaevent")
//      persist(e)(handle)
      persistAsync(e)(handle)
      handle(e)

    case query: Query =>
      log.debug("received query {}", query)
      val space = state.get(query.dimensions)
      log.debug("space is {}", space)
      if(query.immutable)
        immutableSegmentActor forward QueryEvent(query, space)
      else
        segmentActor forward QueryEvent(query, space)

    case GetAll =>
      sender() ! state.getAll

    case SaveSnapshot =>
      log.info("saving snapshot")
      saveSnapshot(state)

    case Shutdown =>
      sender() ! context.stop(self)

    case Debug =>
      state.debug()

    case SaveSnapshotSuccess(metadata) =>
      log.info(s"snapshot saved. seqNum:${metadata.sequenceNr}, timeStamp:${metadata.timestamp}")
      deleteMessages(metadata.sequenceNr)

    case SaveSnapshotFailure(_, reason) =>
      log.info("failed to save snapshot", reason)

    case DeleteMessagesSuccess(toSequenceNr) =>
      log.info(s"message deleted. sequNum {}", toSequenceNr)

    case DeleteMessagesFailure(reason, toSequenceNr) =>
      log.info(s"failed to delete message to sequenceNr: {} {}", toSequenceNr, reason)

    case i: LoadImmutable =>
      immutableSegmentActor forward i.copy(space = Some(state.get(i.bucket)))
//      val f: Iterable[Future[Any]] = state.data.map{case(bucket, space) =>
//        log.info("Passing on space {}", space)
//        immutableSegmentActor ? i.copy(space = Some(space))
//      }
//       TODO maybe not block here?
//      Try(Await.result(Future.sequence(f), Duration.Inf)) match {
//        case Success(list) if list.nonEmpty && list.forall(_ == Ack) =>
//          log.info("successful init")
//          sender() ! Ack
//        case Failure(e) =>
//          log.warn("failed to Initialize", e)
//          sender() ! Nack
//        case _ =>
//          log.warn("failed to Initialize, {}", i)
//          sender() ! Nack
//      }

    case x =>
      log.info(s"received $x")

  }

  override def receiveRecover: Receive = {

    case e: PostMetaEvent =>
      log.debug("received recover postmetaevent")
      handle(e)

    case SnapshotOffer(_, snapshot: DomainState) =>
      log.info("restoring state from snapshot")
      state = snapshot

    case x =>
      log.info("received recover {}", x)

  }

}

object DomainActor {
  def props(audienceDao: ActorRef, immutableSegmentActor: ActorRef, readOnlyMetaActor: Option[ActorRef] = None): Props = {
    Props(new DomainActor(audienceDao, immutableSegmentActor: ActorRef, readOnlyMetaActor))
  }
}



