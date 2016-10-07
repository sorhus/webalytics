package com.github.sorhus.webalytics.akka

import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.{Done, NotUsed}
import akka.actor.{ActorRef, ActorSystem, DeadLetter, Props}
import akka.stream.ActorMaterializer
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.server.Directives
import com.github.sorhus.webalytics.model._
import spray.json.DefaultJsonProtocol
import akka.pattern.ask
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.journal.leveldb.scaladsl.LeveldbReadJournal
import akka.stream.scaladsl.Tcp.{IncomingConnection, ServerBinding}
import akka.stream.scaladsl._
import akka.util.{ByteString, Timeout}

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
  val queryActor: ActorRef = system.actorOf(DimensionValueActor.props(audienceActor), "meta")
  val documentActor: ActorRef = system.actorOf(DocumentIdActor.props(audienceActor, queryActor), "document")

    val welcome = Source.single("OK")

    val echo = Flow[ByteString]
      .via(Framing.delimiter(
        ByteString("\n"),
        maximumFrameLength = Math.pow(2,12).toInt,
        allowTruncation = true))
      .map(_.utf8String)
      .map{msg =>
        val data = Map("d1" -> List("v1","v2"))
        val res = documentActor ? PostEvent1(Bucket("user"), ElementId(UUID.randomUUID().toString), Element.fromMap(data))
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

  implicit val system = ActorSystem("server")
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher
  implicit val timeout = Timeout(10, TimeUnit.SECONDS)


  val deadLetter = system.actorOf(DeadLetterLoggingActor.props(), "dead-letter")
  system.eventStream.subscribe(deadLetter, classOf[DeadLetter])

  val immutableBitsetActor = system.actorOf(ImmutableBitsetActor.props("roaring")) // TODO don't hardcode
  val readOnlyQueryActor = system.actorOf(ReadonlyDimensionValueActor.props(immutableBitsetActor), "readonly-meta")
  val queries = PersistenceQuery(system).readJournalFor[LeveldbReadJournal](LeveldbReadJournal.Identifier)
  val res = queries.eventsByPersistenceId(DimensionValueActor.persistenceId, 0L, Long.MaxValue)
    .runForeach{e =>
      readOnlyQueryActor ! e.event.asInstanceOf[PostMetaEvent]
    }

  val audienceActor: ActorRef = system.actorOf(BitsetAudienceActor.props(immutableBitsetActor), "audience")
//  val audienceActor: ActorRef = system.actorOf(BitsetAudienceActor.props(), "audience")
  val queryActor: ActorRef = system.actorOf(DimensionValueActor.props(audienceActor), "meta")
  val documentActor: ActorRef = system.actorOf(DocumentIdActor.props(audienceActor, queryActor), "document")

  println(audienceActor)
  println(queryActor)
  println(documentActor)


  Thread.sleep(TimeUnit.SECONDS.toMillis(3))

  val route =
    path("debug") {
      get {
        queryActor ! Debug
        audienceActor ! Debug
        complete("")
      }
    } ~
    path("count") {
      get {
        entity(as[JsonQuery]) { jsonQuery =>
          complete {
            (queryActor ? jsonQuery.toQuery)
              .mapTo[Map[String, Map[String, Map[String, Long]]]]
          }
        }
      }
    } ~
      path("count" / "static") {
        get {
          entity(as[JsonQuery]) { jsonQuery =>
            complete {
              (readOnlyQueryActor ? jsonQuery.toQuery)
                .mapTo[Map[String, Map[String, Map[String, Long]]]]
            }
          }
        }
    } ~
      path("post" / Segment / Segment) { case (bucket, elementId) =>
        post {
          entity(as[Map[String,List[String]]]) { data =>
            documentActor ! PostEvent1(Bucket(bucket), ElementId(elementId), Element.fromMap(data))
            complete("")
          }
        }
      } ~
      path("postblocking" / Segment / Segment) { case (bucket, elementId) =>
        post {
          entity(as[Map[String,List[String]]]) { data =>
            val result = documentActor ? PostEvent1(Bucket(bucket), ElementId(elementId), Element.fromMap(data))
            Await.result(result, timeout.duration)
            complete("")
          }
        }

      } ~
      path("close" /  Segment) { case (bucket) =>
        get {
          val q = audienceActor ? CloseBucket(Bucket(bucket))
          val result = Await.result(q, Duration.Inf).toString
          complete(result)
        }
      } ~
      path("snapshot" / "save") {
        post {
          audienceActor ! SaveSnapshot
          queryActor ! SaveSnapshot
          documentActor ! SaveSnapshot
          complete("")
        }
      } ~
      path("snapshot" / Segment) { case (actor) =>
        get {
//          actor match {
//            case "audience" => audienceActor ? LatestSnapshot
//          }
          complete("")
        }
      } ~
      path("immutate" / "init" / Segment) { case (bucket) =>
        post {
          readOnlyQueryActor ! Initialize(Bucket(bucket))
          complete("")
        }
      } ~
      path("immutate" / Segment) { case (bucket) =>
        post {
          audienceActor ! Immutate(Bucket(bucket))
          complete("")
        }
      }



  val bindingFuture = Http().bindAndHandle(route, "localhost", 9000)
  println(s"Server online at http://localhost:9000/\nPress RETURN to stop...")
  StdIn.readLine() // let it run until user presses return
  bindingFuture
    .flatMap(_.unbind()) // trigger unbinding from the port
    .onComplete{_ =>
      documentActor ? Shutdown
      queryActor ? Shutdown
      audienceActor ? Shutdown
      system.terminate
    } // and shutdown when done
}
