package com.github.sorhus.webalytics.akka

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.util.Timeout
import com.github.sorhus.webalytics.akka.document.DocumentIdActor
import com.github.sorhus.webalytics.akka.domain.DomainActor
import com.github.sorhus.webalytics.akka.segment.{ImmutableSegmentActor, SegmentActor}
import com.github.sorhus.webalytics.akka.model._

import scala.concurrent.ExecutionContext

class RoutingActor(documentActor: ActorRef, domainActor: ActorRef, segmentActor: ActorRef) extends Actor {

  // TODO pass these
  implicit val timeout = Timeout(1, TimeUnit.MINUTES)
  implicit val ec = ExecutionContext.Implicits.global

  override def receive: Receive = {

    case e: PostCommand =>
      documentActor forward e

    case query: Query =>
      domainActor forward query

    case SaveSnapshot =>
      domainActor ! SaveSnapshot
      segmentActor ! SaveSnapshot
      documentActor ! SaveSnapshot
      // TODO would be nice to be able to guarantee snapshot success at this point
      sender() ! Ack

    case i: LoadImmutable =>
      domainActor forward i

    case m: MakeImmutable =>
      segmentActor forward m

    case c: CloseBucket =>
      segmentActor forward c
  }
}


object RoutingActor {
  def props()(implicit system: ActorSystem) = {
    val immutableSegmentActor = system.actorOf(ImmutableSegmentActor.props("roaring")) // TODO don't hardcode

    val segmentActor: ActorRef = system.actorOf(SegmentActor.props(immutableSegmentActor), "segment")
    val metaActor: ActorRef = system.actorOf(DomainActor.props(segmentActor, immutableSegmentActor), "domain")

    val documentActor = system.actorOf(DocumentIdActor.props(segmentActor, metaActor, 0, 1))

    Props(new RoutingActor(documentActor, metaActor, segmentActor))
  }
}
