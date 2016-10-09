package com.github.sorhus.webalytics.akka.meta

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, Props}
import com.github.sorhus.webalytics.model._
import akka.pattern.ask
import akka.util.Timeout

import scala.collection.immutable.Iterable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}


class ReadonlyMetaDataActor(audienceActor: ActorRef) extends TMetaDataActor {

  implicit val timeout = Timeout(10, TimeUnit.SECONDS)
  import concurrent.ExecutionContext.Implicits.global

  override def receiveCommand: Receive = {

    case e: PostMetaEvent =>
      log.info("received postmetaevent {}", e)
      handle(e)

    case query: Query =>
      log.info("received query {}", query)
      val space = state.get(query.dimensions)
      log.info("space is {}", space)
      audienceActor forward QueryEvent(query, space)

    case i: Initialize =>
      val f: Iterable[Future[Any]] = state.data.map{case(bucket, space) =>
        audienceActor ? i.copy(space = Some(space))
      }
      Try(Await.result(Future.sequence(f), Duration.Inf)) match {
        case Success(list) =>
          log.info("successful init {}", list)
          sender() ! Ack
        case Failure(e) =>
          log.warn("failed to Initialize", e)
          sender() ! Nack
      }

    case x =>
      println(s"received $x")
  }


}

object ReadonlyMetaDataActor {
  def props(audienceDao: ActorRef): Props = Props(new ReadonlyMetaDataActor(audienceDao))
}
