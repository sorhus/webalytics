package com.github.sorhus.webalytics.akka

import java.io.{BufferedReader, FileReader}
import java.util.UUID

import akka.actor.{Actor, ActorRef, Props}
import akka.util.Timeout
import com.github.sorhus.webalytics.model._
import org.json4s.jackson.JsonMethods._
import org.slf4j.LoggerFactory

class BitsetLoadingActor(postActor: ActorRef) extends Actor {

  val log = LoggerFactory.getLogger(getClass)

  override def receive: Receive = {

//    case l @ Load(file, bucket) =>
//      log.info("received load {}", l)
//      load(file, bucket)
//       TODO get ack?
    case _ => ""

  }

//  def load(file: String, bucket: Bucket)(implicit timeout: Timeout): Unit = {
//    val in = new BufferedReader(new FileReader(file))
//    Stream.continually(in.readLine())
//      .takeWhile(_ != null)
//      .map(_.split("\t"))
//      .map {
//        case Array(elementId, json) =>
//          val element = parse(json)
//            .extract[Map[String, List[String]]]
//            .map { case (dimension: String, values: List[String]) =>
//              Dimension(dimension) -> values.map(v => Value(v))
//            }
//          (ElementId(elementId), Element(element))
//        case Array(json) =>
//          val element = parse(json)
//            .extract[Map[String, List[String]]]
//            .map { case (dimension: String, values: List[String]) =>
//              Dimension(dimension) -> values.map(v => Value(v))
//            }
//          (ElementId(UUID.randomUUID().toString), Element(element))
//      }
//      .foreach { case (elementId, element) =>
//        postActor ! PostEvent1(bucket, elementId, element)
//      }
//    in.close()
//  }
}

object BitsetLoadingActor {
  def apply(postActor: ActorRef) = Props(new BitsetLoadingActor(postActor))
}