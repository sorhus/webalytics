package com.github.sorhus.webalytics.akka.domain

import akka.persistence.{PersistentActor, SnapshotOffer}
import com.github.sorhus.webalytics.akka.event.PostMetaEvent
import org.slf4j.LoggerFactory

trait TDomainActor extends PersistentActor {

  val log = LoggerFactory.getLogger(getClass)

//  var state = DomainState()
  var state = MutableDomainState()

  def handle(e: PostMetaEvent) = {
    state = state.update(e)
  }

  override def persistenceId: String = "domain-actor"

  override def receiveRecover: Receive = {

    case e: PostMetaEvent =>
      log.info("received recover postmetaevent")
      handle(e)

//    case SnapshotOffer(_, snapshot: DomainState) =>
    case SnapshotOffer(_, snapshot: MutableDomainState) =>
      log.info("restoring state from snapshot")
      state = snapshot

    case x =>
      log.info("received recover {}", x)

  }

}
