package com.github.sorhus.webalytics.akka

import akka.actor.{Actor, ActorRef, ActorSystem, DeadLetter, Props}
import akka.pattern.ask
import akka.util.Timeout
import com.github.sorhus.webalytics.akka.document.DocumentIdActor
import com.github.sorhus.webalytics.akka.domain.DomainActor
import com.github.sorhus.webalytics.akka.segment.{ImmutableSegmentActor, SegmentActor}
import com.github.sorhus.webalytics.akka.event._
import com.github.sorhus.webalytics.akka.util.DeadLetterLoggingActor
import com.github.sorhus.webalytics.model.Query

import scala.concurrent.{ExecutionContext, Future}

class RoutingActor(documentActor: ActorRef, domainActor: ActorRef, segmentActor: ActorRef)
                  (implicit ec: ExecutionContext, timeout: Timeout) extends Actor {

  override def receive: Receive = {

    case p: PostCommand =>
      documentActor forward p

    case q: Query =>
      domainActor forward q

    case GetAll =>
      domainActor forward GetAll

    case c: CountCommand =>
      domainActor forward c

    case SaveSnapshot =>
      documentActor ! SaveSnapshot
      sender() ! Ack

    case l: LoadImmutable =>
      domainActor forward l

    case m: MakeImmutable =>
      segmentActor forward m

    case c: CloseBucket =>
      segmentActor forward c

    case Shutdown =>
      val futures = List(
        documentActor ? Shutdown,
        domainActor ? Shutdown,
        segmentActor ? Shutdown
      ).map(_.mapTo[AckOrNack])

      val future = Future.sequence(futures).map(_.reduce(_ * _))
      sender() ! future

  }

}

object RoutingActor {
  def props()(implicit system: ActorSystem, ec: ExecutionContext, timeout: Timeout) = {
    val roaringDir = system.settings.config.getString("akka.actor.roaring.path")

    val deadLetter = system.actorOf(DeadLetterLoggingActor.props(), "dead-letter")
    system.eventStream.subscribe(deadLetter, classOf[DeadLetter])

    val immutableSegmentActor = system.actorOf(ImmutableSegmentActor.props(roaringDir), "immutable-segment")
    val segmentActor: ActorRef = system.actorOf(SegmentActor.props(immutableSegmentActor), "segment")
    val domainActor: ActorRef = system.actorOf(DomainActor.props(segmentActor, immutableSegmentActor), "domain")
    val documentIdActor = system.actorOf(DocumentIdActor.props(segmentActor, domainActor))

    Props(new RoutingActor(documentIdActor, domainActor, segmentActor))
  }
}
