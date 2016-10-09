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
import akka.stream.scaladsl.Tcp.{IncomingConnection, ServerBinding}
import akka.stream.scaladsl._
import akka.util.{ByteString, Timeout}
import com.github.sorhus.webalytics.akka.document.DocumentIdActor
import com.github.sorhus.webalytics.akka.meta.{MetaDataActor, ReadonlyMetaDataActor}
import org.json4s.{DefaultFormats, Formats}
import org.json4s.jackson.JsonMethods._
import org.slf4j.LoggerFactory

import scala.collection.immutable.Seq
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

  val log = LoggerFactory.getLogger(getClass)

  implicit val system = ActorSystem("server")
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher
  implicit val timeout = Timeout(10, TimeUnit.SECONDS)
  implicit val jsonFormats: Formats = DefaultFormats


  val deadLetter = system.actorOf(DeadLetterLoggingActor.props(), "dead-letter")
  system.eventStream.subscribe(deadLetter, classOf[DeadLetter])

  val immutableBitsetActor = system.actorOf(ImmutableBitsetActor.props("roaring")) // TODO don't hardcode
  val immutableMetaActor = system.actorOf(ReadonlyMetaDataActor.props(immutableBitsetActor), "readonly-meta")
//  val queries = PersistenceQuery(system).readJournalFor[LeveldbReadJournal](LeveldbReadJournal.Identifier)
//  val res = queries.eventsByPersistenceId(MetaDataActor.persistenceId, 0L, Long.MaxValue)
//    .runForeach{e =>
//      readOnlyQueryActor ! e.event.asInstanceOf[PostMetaEvent]
//    }

  val audienceActor: ActorRef = system.actorOf(BitsetAudienceActor.props(immutableBitsetActor), "audience")
  val metaActor: ActorRef = system.actorOf(MetaDataActor.props(audienceActor), "meta")
  val documentActor: ActorRef = system.actorOf(DocumentIdActor.props(audienceActor, metaActor), "document")

  Thread.sleep(TimeUnit.SECONDS.toMillis(3))

  val route =
    path("debug") {
      get {
        metaActor ! Debug
        audienceActor ! Debug
        complete("")
      }
    } ~
    path("count") {
      get {
        entity(as[JsonQuery]) { jsonQuery =>
          complete {
            (metaActor ? jsonQuery.toQuery)
              .mapTo[Map[String, Map[String, Map[String, Long]]]]
          }
        }
      }
    } ~
      path("count" / "static") {
        get {
          entity(as[JsonQuery]) { jsonQuery =>
            complete {
              (immutableMetaActor ? jsonQuery.toQuery)
                .mapTo[Map[String, Map[String, Map[String, Long]]]]
            }
          }
        }
    } ~
      path("post" / Segment / Segment) { case (bucket, elementId) =>
        post {
          entity(as[Map[String,Set[String]]]) { data =>
            documentActor ! PostEvent1(Bucket(bucket), ElementId(elementId), Element.fromMap(data))
            complete("")
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
              documentActor ? PostEvent1(Bucket(bucket), elementId, Element.fromMap(data)) map(_.asInstanceOf[AckOrNack].toString)
            }
//            val future: Future[Done] = source.via(flow).runForeach{ e =>
//              documentActor ? e
//            }
            val y: Future[String] = source.via(flow).runWith(Sink.last)
            complete(y)
//            val y: Future[Seq[Future[Any]]] = source.via(flow).toMat(Sink.seq)(Keep.right).run()
//            log.info("started flow", y)
//            Try(Await.result(y, Duration.Inf)) match {
//              case Success(seq) =>
//                log.info("input consumed")
//                Try(Await.result(Future.sequence(seq), Duration.Inf)) match {
//                  case Success(s) =>
//                    log.warn("seq returned {}", s)
//                    complete("")
//                  case Failure(e) =>
//                    log.warn("batch failed", e)
//                    sys.error("")
//                }
//              Try(Await.result(seq, Duration.Inf)) match {
//                case Success(s) =>
//                  log.warn("seq returned {}", s)
//                  complete("")
//                case Failure(e) =>
//                  log.warn("batch failed", e)
//                  sys.error("")
//              }
//
//              case Failure(e) =>
//                log.warn("batch failed", e)
//                sys.error("")
//            }
//            val x = flow.runWith(flow, Sink.seq)
//            complete("")
//            complete(future)
          }
        }
      } ~
      path("postblocking" / Segment / Segment) { case (bucket, elementId) =>
        post {
          entity(as[Map[String,Set[String]]]) { data =>
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
          metaActor ! SaveSnapshot
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
          val future = immutableMetaActor ? Initialize(Bucket(bucket))
          Try(Await.result(future, Duration.Inf)) match {
            case Success(Ack) =>
              complete("")
            case Success(Nack) =>
              log.warn("immutate failed with Nack")
              sys.error("")
            case Failure(e) =>
              log.warn("immutate failed", e)
              sys.error("")
          }
        }
      } ~
      path("immutate" / Segment) { case (bucket) =>
        post {
          val future = audienceActor ? Immutate(Bucket(bucket))
          Try(Await.result(future, Duration.Inf)) match {
            case Success(Ack) => complete("")
            case Failure(e) =>
              log.warn("immutate failed", e)
              sys.error("")
          }
        }
      }



  val bindingFuture = Http().bindAndHandle(route, "localhost", 9000)
  println(s"Server online at http://localhost:9000/\nPress RETURN to stop...")
  StdIn.readLine() // let it run until user presses return
  bindingFuture
    .flatMap(_.unbind()) // trigger unbinding from the port
    .onComplete{_ =>
      documentActor ? Shutdown
      metaActor ? Shutdown
      audienceActor ? Shutdown
      system.terminate
    } // and shutdown when done
}
