package com.github.sorhus.webalytics.client

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.HttpMessage.DiscardedEntity
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.ByteString
import com.github.sorhus.webalytics.akka.model.{Dimension, Element, ElementId, Value}
import org.json4s.{Formats, JValue}
import org.json4s.jackson.{Json, Serialization}
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}
import org.json4s.jackson.JsonMethods._

object A extends App {
  val w = new Webalytics
  implicit val ec = w.executionContext
  println(Await.result(w.getAll(), Duration.Inf))

  val batch = w.batchPost(List((ElementId(), Element.fromMap(Map("d1" -> Set("v1","v2"))))), "test")
  Await.result(Future.sequence(batch), Duration.Inf)

  println(Await.result(w.getAll(), Duration.Inf))
}

class Webalytics extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher
  implicit val formats = org.json4s.DefaultFormats

  val url = "http://localhost:9000"

  def getAll() = {
    val flow: Flow[ByteString, Map[String, Map[String, Map[String, Long]]], NotUsed] = Flow[ByteString].map{bytes =>
      parse(bytes.utf8String)
        .extract[Map[String, Map[String, Map[String, Long]]]]
    }

    Http().singleRequest(HttpRequest(HttpMethods.POST, uri = s"$url/count/all"))
      .flatMap { response =>
        response.entity
          .dataBytes
          .via(flow)
          .runWith(Sink.head)
      }
  }

  def batchPost(elements: Iterable[(ElementId, Element)], bucket: String, batchSize: Int = 10000): List[Future[DiscardedEntity]] = {
    elements.grouped(batchSize)
      .map{batch =>
        val data = batch.toList.map{case(elementId, element) =>
          s"$elementId\t${Serialization.write(Element.toMap(element))})"
        }.mkString("\n") // TODO clean up this mess
        Http().singleRequest(HttpRequest(HttpMethods.POST, s"$url/batch/post/$bucket", entity = HttpEntity(data)))
          .map(_.discardEntityBytes(materializer))
      }
      .toList
  }

}
