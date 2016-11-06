package com.github.sorhus.webalytics.client

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.MediaTypes._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink}
import akka.util.ByteString
import com.github.sorhus.webalytics.model._
import org.json4s.jackson.Serialization
import spray.json.DefaultJsonProtocol

import scala.concurrent.{Await, Future}
import org.json4s.jackson.JsonMethods._

import scala.concurrent.duration.Duration

class QueryBuilder {

}



class Webalytics extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher
  implicit val formats = org.json4s.DefaultFormats

  val url = "http://localhost:9000"

  def getAll(): Map[String, Map[String, Map[String, Long]]] = {
    val flow: Flow[ByteString, Map[String, Map[String, Map[String, Long]]], NotUsed] = Flow[ByteString].map{bytes =>
      parse(bytes.utf8String)
        .extract[Map[String, Map[String, Map[String, Long]]]]
    }

    val f = Http().singleRequest(HttpRequest(HttpMethods.POST, uri = s"$url/count/all"))
      .flatMap { response =>
        response.entity
          .dataBytes
          .via(flow)
          .runWith(Sink.head)
      }

    Await.result(f, Duration.Inf)
  }

  def batchPost(elements: Iterable[(ElementId, Element)], bucket: String, batchSize: Int = 10000): Unit = {
    val f = elements.grouped(batchSize)
      .map{batch =>
        val data = batch.toList.map{case(elementId, element) =>
          s"$elementId\t${Serialization.write(Element.toMap(element))})"
        }.mkString("\n") // TODO clean up this mess
        Http().singleRequest(HttpRequest(HttpMethods.POST, s"$url/batch/post/$bucket", entity = HttpEntity(data)))
          .map(_.discardEntityBytes(materializer))
      }
      .toList

    Await.result(Future.sequence(f), Duration.Inf)
  }

  def get(query: Query) = {
    val flow: Flow[ByteString, Map[String, Map[String, Map[String, Long]]], NotUsed] = Flow[ByteString].map{bytes =>
      parse(bytes.utf8String)
        .extract[Map[String, Map[String, Map[String, Long]]]]
    }

    val data = Serialization.write(JsonQuery.fromQuery(query))
    val f = Http().singleRequest(HttpRequest(HttpMethods.POST, uri = s"$url/count", entity = HttpEntity(`application/json`, data)))
      .flatMap { response =>
        response.entity
          .dataBytes
          .via(flow)
          .runWith(Sink.head)
      }

    Await.result(f, Duration.Inf)

  }
}
