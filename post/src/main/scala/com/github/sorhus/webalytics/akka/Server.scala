package com.github.sorhus.webalytics.akka

import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.actor.{ActorRef, ActorSystem, DeadLetter}
import akka.stream.ActorMaterializer
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.server.Directives
import com.github.sorhus.webalytics.model._
import spray.json.DefaultJsonProtocol
import akka.pattern.ask
import akka.stream.scaladsl._
import akka.util.{ByteString, Timeout}
import com.github.sorhus.webalytics.akka.document.RoutingActor
import com.github.sorhus.webalytics.akka.segment.MakeImmutable
import org.json4s.{DefaultFormats, Formats}
import org.json4s.jackson.JsonMethods._
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.io.StdIn

trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val jsonQueryFormat = jsonFormat3(JsonQuery)
  implicit val valueFormat = jsonFormat1(Value)
  implicit val dimensionFormat = jsonFormat1(Dimension)
  implicit val bucketFormat = jsonFormat1(Bucket)
}

object Server extends App with Directives with JsonSupport {

  val log = LoggerFactory.getLogger(getClass)

  implicit val system = ActorSystem("server")
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher
  implicit val timeout = Timeout(1, TimeUnit.MINUTES)
  implicit val jsonFormats: Formats = DefaultFormats


  val deadLetter = system.actorOf(DeadLetterLoggingActor.props(), "dead-letter")
  system.eventStream.subscribe(deadLetter, classOf[DeadLetter])

  val routingActor: ActorRef = system.actorOf(RoutingActor.props(), "routing")


  val route =
//    path("debug") {
//      get {
//        metaActor ! Debug
//        audienceActor ! Debug
//        complete("")
//      }
//    } ~
    path("count") {
      post {
        entity(as[JsonQuery]) { jsonQuery =>
          complete {
            (routingActor ? jsonQuery.toQuery)
              .mapTo[Map[String, Map[String, Map[String, Long]]]]
          }
        }
      }
    } ~
      path("count" / "immutable") {
        get {
          entity(as[JsonQuery]) { jsonQuery =>
            complete {
              (routingActor ? jsonQuery.toQuery.copy(immutable = true))
                .mapTo[Map[String, Map[String, Map[String, Long]]]]
            }
          }
        }
    } ~
      path("post" / Segment / Segment) { case (bucket, elementId) =>
        post {
          entity(as[Map[String,Set[String]]]) { data =>
            complete {
              routingActor ? PostCommand(Bucket(bucket), ElementId(elementId), Element.fromMap(data)) map(_.toString)
            }
          }
        }
      } ~
      path("batch"/ "post" / Segment ) { case (bucket) =>
        post {
          extractDataBytes { (source: Source[ByteString, Any]) =>
            val delim = Framing.delimiter(ByteString("\n"),maximumFrameLength = Int.MaxValue,allowTruncation = true)
            val flow: Flow[ByteString, String, NotUsed] = Flow[ByteString].via(delim).mapAsync(10){ bytes =>
              val (elementId, json) = bytes.utf8String.split("\t") match {
                case Array(json) => (ElementId(), json)
                case Array(elementId, json) => (ElementId(elementId), json)
              }
              val data = parse(json).extract[Map[String, Set[String]]]
              (routingActor ? PostCommand(Bucket(bucket), elementId, Element.fromMap(data)))
                .mapTo[AckOrNack]
                .map(_.toString)
            }
            val y: Future[String] = source.via(flow).runWith(Sink.last)
            complete(y)
          }
        }
      } ~
      path("close" /  Segment) { case (bucket) =>
        get {
          complete {
            (routingActor ? CloseBucket(Bucket(bucket)))
              .mapTo[AckOrNack]
              .flatMap{
                case Ack => routingActor ? SaveSnapshot
                case Nack => Future(Nack)
              }
              .mapTo[AckOrNack] // TODO make marshalleable
              .map(_.toString)
          }
        }
      } ~
      path("snapshot" / "save") {
        post {
          complete {
            (routingActor ? SaveSnapshot)
              .mapTo[AckOrNack]
              .map(_.toString)
          }
        }
      } ~
      path("loadimmutable" / Segment) { case (bucket) =>
        post {
          complete {
            (routingActor ? LoadImmutable(Bucket(bucket)))
              .mapTo[AckOrNack]
              .map(_.toString)
          }
        }
      } ~
      path("makeimmutable" / Segment) { case (bucket) =>
        post {
          complete {
            (routingActor ? MakeImmutable(bucket = Bucket(bucket)))
              .mapTo[AckOrNack]
              .map(_.toString)
          }
        }
      }



  val bindingFuture = Http().bindAndHandle(route, "localhost", 9000)
  println(s"Server online at http://localhost:9000/\nPress RETURN to stop...")
  StdIn.readLine() // let it run until user presses return
  bindingFuture
    .flatMap(_.unbind()) // trigger unbinding from the port
    .onComplete{_ =>
//      documentActor ? Shutdown
//      metaActor ? Shutdown
//      audienceActor ? Shutdown
      system.terminate
    } // and shutdown when done
}
