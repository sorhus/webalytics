package com.github.sorhus.webalytics.api

import akka.actor.ActorSystem
import com.github.sorhus.webalytics.impl.SparseBitSetWrapper
import com.github.sorhus.webalytics.impl.redis.RedisMetaDao
import com.github.sorhus.webalytics.model._
import com.zaxxer.sparsebits.SparseBitSet
import org.json4s.jackson.Serialization
import org.scalatra.{AsyncResult, FutureSupport, Params, ScalatraServlet}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class Servlet(dao: AudienceDao)(implicit system: ActorSystem, metaDao: MetaDao) extends ScalatraServlet with FutureSupport {

  val log = LoggerFactory.getLogger(getClass)

  override protected implicit def executor: ExecutionContext = system.dispatcher

  implicit val jsonFormats: Formats = DefaultFormats
//  import implicit My

  val bucket_ = "bucket"
  val element_id_ = "element_id"

  def getElement(params: Params) = {
    val attempt = Try {
      params
        .keys
        .filter(k => k != bucket_ && k != element_id_)
        .head
    }
      .map(json => parse(json))
      .map(_.extract[Map[String, List[String]]])

    if(attempt.isFailure) {
      log.info("could not parse element", attempt.failed.get)
    }

    attempt.toOption
      .map{ (s: Map[String, List[String]]) =>
        s.map{case(dimension: String, values: List[String]) =>
          Dimension(dimension) -> values.map(v => Value(v)).toSet
        }
      }
      .map(e => Element(e))

  }

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
        element.foreach(d => dao.post(bucket, element_id, d))
        Future.successful("") // TODO be more rigorous
      }
    }
  }

  // example request
  // curl -XPOST localhost:8080/count -d '{"filter":[[{"user":{"referrer":["*"],"section":["*"]}}]],"buckets":["user"],"dimensions":["*"]}'
  post(s"/count") {
    new AsyncResult {
      val is: Future[String] = Future {
        val query: Option[Query] = getQuery(params)
        query.map(dao.getCount)
          .map(JsonResult.fromResult)
          .map(res => Serialization.write(res))
          .getOrElse("error")
      }
    }
  }

}