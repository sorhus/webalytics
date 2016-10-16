package com.github.sorhus.webalytics.akka

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.persistence.PersistentActor
import akka.util.Timeout
import com.github.sorhus.webalytics.akka.document.DocumentIdActor
import com.github.sorhus.webalytics.akka.domain.DomainActor
import com.github.sorhus.webalytics.akka.segment.{ImmutableSegmentActor, SegmentActor}
import com.github.sorhus.webalytics.akka.model._

import scala.concurrent.ExecutionContext

class ActorContainer(documentActor: ActorRef, domainActor: ActorRef, segmentActor: ActorRef)
                    (implicit ec: ExecutionContext, timeout: Timeout) extends PersistentActor {

  var state = RoutingState()

  override def receiveCommand: Receive = {

    case p: PostCommand =>
      documentActor forward p

    case q: Query =>
      domainActor forward q

    case SaveSnapshot =>
      domainActor ! SaveSnapshot
      segmentActor ! SaveSnapshot
      documentActor ! SaveSnapshot
      // TODO would be nice to be able to guarantee snapshot success at this point
      sender() ! Ack

    case l: LoadImmutable =>
      persist(l){ _ =>
        state = state.add(l.bucket)
        domainActor forward l
      }

    case m: MakeImmutable =>
      segmentActor forward m

    case c: CloseBucket =>
      segmentActor forward c

  }

  override def receiveRecover: Receive = {
    case l: LoadImmutable =>
      state = state.add(l.bucket)
  }

  override def persistenceId: String = "actor-container"
}

case class RoutingState(private val immutables: Set[Bucket] = Set.empty) {
  def immutable(b: Bucket) = immutables.contains(b)
  def add(b: Bucket) = copy(immutables + b)
}

object ActorContainer {
  def props()(implicit system: ActorSystem, ec: ExecutionContext, timeout: Timeout) = {
    val roaringDir = "roaring" // TODO don't hardcode

    val immutableSegmentActor = system.actorOf(ImmutableSegmentActor.props(roaringDir), "immutable-segment")
    val segmentActor: ActorRef = system.actorOf(SegmentActor.props(immutableSegmentActor), "segment")
    val domainActor: ActorRef = system.actorOf(DomainActor.props(segmentActor, immutableSegmentActor), "domain")
    val documentIdActor = system.actorOf(DocumentIdActor.props(segmentActor, domainActor))

    Props(new ActorContainer(documentIdActor, domainActor, segmentActor))
  }
}
