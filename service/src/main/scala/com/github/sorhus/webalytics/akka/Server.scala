package com.github.sorhus.webalytics.akka

import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.actor.{ActorRef, ActorSystem}
import akka.stream.ActorMaterializer
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.server.Directives
import com.github.sorhus.webalytics.akka.event._
import spray.json.DefaultJsonProtocol
import akka.pattern.ask
import akka.stream.scaladsl._
import akka.util.{ByteString, Timeout}
import com.github.sorhus.webalytics.model._
import org.json4s.{DefaultFormats, Formats}
import org.json4s.jackson.JsonMethods._
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val jsonQueryFormat = jsonFormat3(JsonQuery.apply)
//  implicit val jsonDimensionFormat = jsonFormat1(Dimension)
//  implicit val jsonValueFormat: RootJsonFormat[Value] = jsonFormat1(Value)
//  implicit val jsonElementFormat: RootJsonFormat[Element] = jsonFormat1(Element.apply)
}

object Server extends App with Directives with JsonSupport {

  val PROD = args.headOption.getOrElse("") == "PROD"

  val log = LoggerFactory.getLogger(getClass)

  implicit val system = ActorSystem("server")

  log.debug("settings: {}", system.settings)

  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher
  implicit val timeout = Timeout(1, TimeUnit.MINUTES)
  implicit val jsonFormats: Formats = DefaultFormats

  val routingActor: ActorRef = system.actorOf(RoutingActor.props(), "routing")

    val route =
//    path("debug") {
//      get {
//        metaActor ! Debug
//        audienceActor ! Debug
//        complete("")
//      }
//    } ~
      path("") {
        get {
          getFromResource("static/index.html")
        }
      } ~
      path("static" / Segment) { file =>
        get {
          if(PROD) {
            getFromResource(s"static/$file")
          } else {
            getFromDirectory(s"service/src/main/resources/static/$file") // hot load in dev mode
          }
        }
      } ~
      path("domain") {
        get {
          complete {
            (routingActor ? GetAll)
              .mapTo[Element]
              .map(_.e)
              .map(m => m.map{case(d,v) => d.d -> v.map(vv => vv.v)})
          }
        }
      } ~
      path("count") {
        get {
//          entity(as[String]) {
          parameters('bucket.as[String]) { b =>
            complete {
              val bucket = Bucket(b)
              (routingActor ? Root.query(bucket))
                .mapTo[Map[String, Map[String, Map[String, Long]]]]
                .map(map => Try(map(bucket.b)(Root.dimension.d)(Root.value.v)).getOrElse(0L).toString)
            }
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
      path("count" / "all") {
        post {
          complete {
            (routingActor ? CountCommand())
                .mapTo[Map[String, Map[String, Map[String, Long]]]]
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
              (routingActor ? PostCommand(Bucket(bucket), ElementId(elementId), Root.element))
                .mapTo[AckOrNack]
                .map(_.toString)
            }
          }
        }
      } ~
      path("batch"/ "post" / Segment ) { case (bucket) =>
        post {
          extractDataBytes { (source: Source[ByteString, Any]) =>
            val delim = Framing.delimiter(ByteString("\n"), maximumFrameLength = Int.MaxValue, allowTruncation = true)
            val flow: Flow[ByteString, AckOrNack, NotUsed] = Flow[ByteString].via(delim).mapAsync(10) { bytes =>
              val (elementId, json) = bytes.utf8String.split("\t") match {
                case Array(js) => (ElementId(), js)
                case Array(id, js) => (ElementId(id), js)
              }
              val data = parse(json).extract[Map[String, Set[String]]]
              (routingActor ? PostCommand(Bucket(bucket), elementId, Element.fromMap(data), persist = false))
                .mapTo[AckOrNack]
            }
            val y: Future[String] = source.via(flow).runWith(Sink.seq).flatMap { seq =>
              seq.reduce(_ * _) match {
//                case Ack => Future.successful(Ack.toString)
                case Ack =>
                  val snapshot = (routingActor ? SaveSnapshot).mapTo[AckOrNack]
                  snapshot.map {
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
      path("batch"/ "post" / "basic" / Segment ) { case (bucket) =>
        post {
          extractDataBytes { (source: Source[ByteString, Any]) =>
            val delim = Framing.delimiter(ByteString("\n"), maximumFrameLength = Int.MaxValue, allowTruncation = true)
            val flow: Flow[ByteString, AckOrNack, NotUsed] = Flow[ByteString].via(delim).mapAsync(10) { bytes =>
              (routingActor ? PostCommand(Bucket(bucket), ElementId(bytes.utf8String), Root.element, persist = false))
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


  val port = 9000
  val bindingFuture = Http().bindAndHandle(route, "localhost", port)
  println(s"Server online at http://localhost:$port/\nPress RETURN to stop...")

  Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
    override def run(): Unit = {
      log.info("Executing shutdown hook")
      shutdown()
      log.info("Shutdown hook complete")
    }
  }))

  while(true) {
    Thread.sleep(3000)
  }

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
