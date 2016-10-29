package com.github.sorhus.webalytics.akka.domain

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, Props}
import com.github.sorhus.webalytics.akka.model._
import akka.pattern.ask
import akka.persistence.SnapshotOffer
import akka.util.Timeout

import scala.collection.immutable.Iterable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}


class ReadOnlyDomainActor(audienceActor: ActorRef) extends TDomainActor {

  implicit val timeout = Timeout(10, TimeUnit.SECONDS)
  import concurrent.ExecutionContext.Implicits.global

  override def receiveCommand: Receive = {

    case query: Query =>
      log.info("received query {}", query)
      val space = state.get(query.dimensions)
      log.info("space is {}", space)
      audienceActor forward QueryCommand(query, space)

    case i: LoadImmutable =>
//      val f: Iterable[Future[Any]] = state.data.map{case(bucket, space) =>
//        log.info("Passing on space {}", space)
//        audienceActor ? i.copy(space = Some(space))
//      }
      val space = state.getAll
      log.info("Passing on space {}", space)
      val f = audienceActor ? i.copy(space = Some(space))
//      Try(Await.result(Future.sequence(f), Duration.Inf)) match {
//    case Success(list) if list.forall(_ == Ack) =>
//      log.info("successful init {}",list)
//      sender() ! Ack
//    case Failure(e) =>
//      log.warn("failed to Initialize", e)
//      sender() ! Nack
//    case _ =>
//      log.warn("failed to Initialize")
//      sender() ! Nack
//  }
      Try(Await.result(f, Duration.Inf)) match {
        case Success(Ack) =>
          log.info("successful init")
          sender() ! Ack
        case _ =>
          log.warn("failed to Initialize")
          sender() ! Nack
      }

    case x =>
      println(s"received $x")
  }

  override def receiveRecover: Receive = {

//    case SnapshotOffer(_, snapshot: DomainState) =>
    case SnapshotOffer(_, snapshot: MutableDomainState) =>
      log.info("restoring state from snapshot")
      state = snapshot

  }



}

object ReadOnlyDomainActor {
  def props(audienceDao: ActorRef): Props = Props(new ReadOnlyDomainActor(audienceDao))
}
