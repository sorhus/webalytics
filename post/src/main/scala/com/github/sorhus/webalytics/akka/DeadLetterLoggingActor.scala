package com.github.sorhus.webalytics.akka

import akka.actor.{Actor, ActorRef, DeadLetter, Props}
import org.slf4j.LoggerFactory

class DeadLetterLoggingActor extends Actor {

  val log = LoggerFactory.getLogger(getClass)

  var count = 0
  def receive = {
    case DeadLetter(msg, from, to) =>
      count += 1
      log.info(s"received dead letter no $count, {}", (msg, from, to))
  }
}

object DeadLetterLoggingActor {
  def props(): Props = Props(new DeadLetterLoggingActor)
}
