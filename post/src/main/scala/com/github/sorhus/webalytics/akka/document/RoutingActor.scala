package com.github.sorhus.webalytics.akka.document

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.routing.{ActorRefRoutee, RoundRobinRoutingLogic, Router}
import com.github.sorhus.webalytics.akka.domain.{DomainActor, ReadOnlyDomainActor}
import com.github.sorhus.webalytics.akka.segment.{ImmutableBitsetActor, MakeImmutable, SegmentActor}
import com.github.sorhus.webalytics.model._

class RoutingActor(router: Router, metaActor: ActorRef, audienceActor: ActorRef, readonlyMetaActor: ActorRef) extends Actor {

  override def receive: Receive = {

    case e: PostCommand =>
      router.route(e, sender())

    case query: Query =>
      if(query.immutable) {
        readonlyMetaActor forward query
      } else {
        metaActor forward query
      }

    case SaveSnapshot =>
      router.routees.foreach(r => r.send(SaveSnapshot, sender()))
      metaActor ! SaveSnapshot
      audienceActor ! SaveSnapshot
      // TODO make sure success
      sender() ! Ack

    case i: LoadImmutable =>
      readonlyMetaActor forward i

    case m: MakeImmutable =>
      audienceActor forward m

    case c: CloseBucket =>
      audienceActor forward c
  }
}


object RoutingActor {
  def props()(implicit system: ActorSystem) = {
    val immutableBitsetActor = system.actorOf(ImmutableBitsetActor.props("roaring")) // TODO don't hardcode
    val readonlyMetaActor = system.actorOf(ReadOnlyDomainActor.props(immutableBitsetActor), "readonly-meta")

    val audienceActor: ActorRef = system.actorOf(SegmentActor.props(immutableBitsetActor), "audience")
    val metaActor: ActorRef = system.actorOf(DomainActor.props(audienceActor, Some(readonlyMetaActor)), "meta")
    val n = 1
    val routees = Range(0,n).map{ id =>
      ActorRefRoutee(
        system.actorOf(DocumentIdActor.props(audienceActor, metaActor, id, n), s"document-$id")
      )
    }

    val router = Router(RoundRobinRoutingLogic(), routees)

    Props(new RoutingActor(router, metaActor, audienceActor, readonlyMetaActor))
  }
}
