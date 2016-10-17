package com.github.sorhus.webalytics.akka

import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.actor.{ActorRef, ActorSystem, DeadLetter}
import akka.stream.ActorMaterializer
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.server.Directives
import com.github.sorhus.webalytics.akka.model._
import spray.json.DefaultJsonProtocol
import akka.pattern.ask
import akka.stream.scaladsl._
import akka.util.{ByteString, Timeout}
import org.json4s.{DefaultFormats, Formats}
import org.json4s.jackson.JsonMethods._
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.io.StdIn
import scala.util.{Failure, Success}

trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val jsonQueryFormat = jsonFormat3(JsonQuery)
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
      path("count"/ Segment) { bucket =>
        post {
            complete {
              val b = Bucket(bucket)
              (routingActor ? Query(Filter((Map(b -> Element.root) :: Nil) :: Nil), b :: Nil, Dimension("root") :: Nil))
                .mapTo[Map[String, Map[String, Map[String, Long]]]]
            }
        }
      } ~
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
      path("post" / Segment / Segment) { case (bucket, elementId) =>
        post {
          entity(as[Map[String,Set[String]]]) { data =>
            complete {
              routingActor ? PostCommand(Bucket(bucket), ElementId(elementId), Element.fromMap(data)) map(_.toString)
            }
          }
        }
      } ~
      path("post" / "basic" / Segment / Segment ) { case (bucket, elementId) =>
        post {
          entity(as[String]) { data =>
            complete {
              (routingActor ? PostCommand(Bucket(bucket), ElementId(elementId), Element.root))
                .mapTo[AckOrNack]
                .map(_.toString)
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
      path("batch"/ "post" / "basic" / Segment ) { case (bucket) =>
        post {
          extractDataBytes { (source: Source[ByteString, Any]) =>
            val delim = Framing.delimiter(ByteString("\n"), maximumFrameLength = Int.MaxValue, allowTruncation = true)
            val flow: Flow[ByteString, AckOrNack, NotUsed] = Flow[ByteString].via(delim).mapAsync(10){ bytes =>
              (routingActor ? PostCommand(Bucket(bucket), ElementId(bytes.utf8String), Element.root, persist = false))
                .mapTo[AckOrNack]
            }
            val y: Future[String] = source.via(flow).runWith(Sink.seq).flatMap{seq =>
              seq.reduce(_ * _) match {
                case Ack =>
                  val snapshot = (routingActor ? SaveSnapshot).mapTo[AckOrNack]
                  snapshot.map{
                    case Nack => throw new RuntimeException()
                    case Ack => Ack.toString
                }
                case Nack => Future.failed(new RuntimeException())
              }
            }
            complete(y)
          }
        }
      } ~
      path("close" /  Segment) { case (bucket) =>
        post {
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
            val b = Bucket(bucket)
            (routingActor ? MakeImmutable(bucket = b))
              .mapTo[AckOrNack]
              .flatMap{
                case Ack => routingActor ? LoadImmutable(bucket = b)
                case Nack => Future.successful(Nack)
              }.mapTo[AckOrNack]
              .flatMap{
                case Ack => routingActor ? CloseBucket(b)
                case Nack => Future.successful(Nack)
              }.mapTo[AckOrNack]
              .map(_.toString)
          }
        }
      } ~
      path("shutdown") {
        post {
          shutdown()
          complete("")
        }
      }


  val bindingFuture = Http().bindAndHandle(route, "localhost", 9000)
  println(s"Server online at http://localhost:9000/\nPress RETURN to stop...")
  StdIn.readLine() // let it run until user presses return
  shutdown()

  def shutdown(): Unit = {
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete{_ =>
      val f: Future[Future[AckOrNack]] = (routingActor ? Shutdown).mapTo[Future[AckOrNack]]
      f.flatMap(f => f).onComplete{
        case Success(ackOrNack) => log.info("shutdown returned: {}", ackOrNack)
        case Failure(e) => log.warn("shutdown failed", e)
      }
      system.terminate
    } // and shutdown when done

  }
}
