package com.github.sorhus.webalytics.akka

import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.{Done, NotUsed}
import akka.actor.{ActorRef, ActorSystem, DeadLetter, Props}
import akka.stream.ActorMaterializer
import akka.http.scaladsl.Http
import akka.http.scaladsl.common.NameReceptacle
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.server.Directives
import com.github.sorhus.webalytics.model._
import spray.json.DefaultJsonProtocol
import akka.pattern.ask
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.journal.leveldb.scaladsl.LeveldbReadJournal
import akka.routing.{ActorRefRoutee, ConsistentHashingRoutingLogic, RoundRobinRoutingLogic, Router}
import akka.stream.scaladsl.Tcp.{IncomingConnection, ServerBinding}
import akka.stream.scaladsl._
import akka.util.{ByteString, Timeout}
import com.github.sorhus.webalytics.akka.document.{DocumentIdActor, RoutingActor}
import com.github.sorhus.webalytics.akka.meta.{MetaDataActor, ReadonlyMetaDataActor}
import org.json4s.{DefaultFormats, Formats}
import org.json4s.jackson.JsonMethods._
import org.slf4j.LoggerFactory

import scala.collection.immutable.{IndexedSeq, Seq}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.io.StdIn
import scala.util.{Failure, Success, Try}

trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val jsonQueryFormat = jsonFormat3(JsonQuery)
  implicit val valueFormat = jsonFormat1(Value)
  implicit val dimensionFormat = jsonFormat1(Dimension)
  implicit val bucketFormat = jsonFormat1(Bucket)
}

object Server2 extends App with JsonSupport {

  import spray.json._
  implicit val system = ActorSystem("server")
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher
  implicit val timeOut = Timeout(10, TimeUnit.SECONDS)

  val deadLetter = system.actorOf(DeadLetterLoggingActor.props(), "dead-letter")
  system.eventStream.subscribe(deadLetter, classOf[DeadLetter])

  val audienceActor: ActorRef = system.actorOf(BitsetAudienceActor.props(null), "audience")
  val queryActor: ActorRef = system.actorOf(MetaDataActor.props(audienceActor), "meta")
  val documentActor: ActorRef = system.actorOf(DocumentIdActor.props(audienceActor, queryActor), "document")

  val welcome = Source.single("OK")

  val echo = Flow[ByteString]
    .via(Framing.delimiter(
      ByteString("\n"),
      maximumFrameLength = Math.pow(2,12).toInt,
      allowTruncation = true))
    .map(_.utf8String)
    .map{msg =>
      val data = Map("d1" -> Set("v1","v2"))
      val res = documentActor ? PostCommand(Bucket("user"), ElementId(UUID.randomUUID().toString), Element.fromMap(data))
      s"Server hereby responds to message: $data\n"
    }
    .merge(welcome)
    .map(_ + "\n")
    .map(ByteString(_))

  val connections: Source[IncomingConnection, Future[ServerBinding]] =
    Tcp().bind("127.0.0.1", 9001)

  val connectionHandler: Sink[IncomingConnection, Future[Done]] =
    Sink.foreach[Tcp.IncomingConnection] { conn =>
      println(s"Incoming connection from: ${conn.remoteAddress}")
      conn.handleWith(echo)
    }


  val binding: Future[Tcp.ServerBinding] = connections.to(connectionHandler).run()

  binding onComplete {
    case Success(b) =>
      println(s"Server started, listening on: ${b.localAddress}")
    case Failure(e) =>
      println(s"Server could not be bound: ${e.getMessage}")
  }
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

//  val immutableBitsetActor = system.actorOf(ImmutableBitsetActor.props("roaring")) // TODO don't hardcode
//  val immutableMetaActor = system.actorOf(ReadonlyMetaDataActor.props(immutableBitsetActor), "readonly-meta")
//  val queries = PersistenceQuery(system).readJournalFor[LeveldbReadJournal](LeveldbReadJournal.Identifier)
//  val res = queries.eventsByPersistenceId(MetaDataActor.persistenceId, 0L, Long.MaxValue)
//    .runForeach{e =>
//      readOnlyQueryActor ! e.event.asInstanceOf[PostMetaEvent]
//    }

//  val audienceActor: ActorRef = system.actorOf(BitsetAudienceActor.props(immutableBitsetActor), "audience")
//  val metaActor: ActorRef = system.actorOf(MetaDataActor.props(audienceActor), "meta")
//  val documentActor: ActorRef = system.actorOf(DocumentIdActor.props(audienceActor, metaActor), "document")
  val routingActor: ActorRef = system.actorOf(RoutingActor.props(), "routing")

//  val routees = Range(0,3).map{ id =>
//    ActorRefRoutee(
//      system.actorOf(DocumentIdActor.props(audienceActor, metaActor, id), s"document-$id")
//    )
//  }
//
//  val router = Router(RoundRobinRoutingLogic(), routees)

  Thread.sleep(TimeUnit.SECONDS.toMillis(3))

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
            (routingActor ? Initialize(Bucket(bucket)))
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
