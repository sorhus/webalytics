package com.github.sorhus.webalytics.post

import akka.actor.ActorSystem
import org.json4s.jackson.Serialization
import org.scalatra.{Params, AsyncResult, FutureSupport, ScalatraServlet}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import redis.protocol.MultiBulk

import scala.concurrent.{Future, ExecutionContext}
import scala.util.Try

class Servlet(implicit system: ActorSystem) extends ScalatraServlet with FutureSupport {

//  implicit val dao: AudienceDao = new RedisDao()
  implicit val dao:  BitSetDao = new BitSetDao
  implicit val metaDao: MetaDao = new RedisMetaDao()

  override protected implicit def executor: ExecutionContext = system.dispatcher

  implicit val jsonFormats: Formats = DefaultFormats

  val bucket_ = "bucket"
  val element_id_ = "element_id"

  def getElement(params: Params) = {
    val x: Option[Map[String, List[String]]] = Try {
      params
        .keys
        .filter(k => k != bucket_ && k != element_id_)
        .head
    }
      .map(json => parse(json))
      .map(_.extract[Map[String, List[String]]])
      .toOption

    val e: Option[Map[Dimension, List[Value]]] = x.map{ (s: Map[String, List[String]]) =>
      s.map{case(dimension: String, values: List[String]) =>
        Dimension(dimension) -> values.map(v => Value(v))
      }
    }

    e.map(e => Element(e))

  }

  //    .map{case(dimension, values) =>
  //      Dimension(dimension) -> values.map(v => Value.apply(v))
  //    }

  def getQuery(params: Params) = Try {
    params
      .keys
      .head
  }
  .map(json => parse(json))
  .map{json => json.extract[JsonQuery].toQuery}
  .toOption

  // example request
  // curl -g -XPOST localhost:8080/post/user/dc415c6b-5564-40b9-95bb-ebdf0d1560e4 -d '{"section":["3f063265","673a3a2d"],"referrer":["twitter"]}'
  post(s"/post/:${bucket_}/:$element_id_") {
    new AsyncResult {
      val is: Future[String] = {
        val bucket = Bucket(params(bucket_))
        val element_id = ElementId(params(element_id_))
        val element: Option[Element] = getElement(params)
        println(s"$bucket, $element_id, $element")
        element.foreach(d => dao.post(bucket, element_id, d))
        dao.debug
        Future.successful("") // TODO be more rigorous
      }
    }
  }

  // example request
  // curl -XPOST localhost:8080/count -d '{"filter":[[{"user":{"referrer":["*"],"section":["*"]}}]],"buckets":["user"],"dimensions":["*"]}'
  post(s"/count") {
    new AsyncResult {
      val is: Future[String] = Future {
        dao.debug
        val query: Option[Query] = getQuery(params)
        println(query)
        query.map(dao.getCount)
          .map{r =>
            println(r)
            Serialization.write(r)
          }
          .getOrElse("error")
      }
    }
  }

}