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

  val dao = new Dao()

  override protected implicit def executor: ExecutionContext = system.dispatcher
  implicit val jsonFormats: Formats = DefaultFormats

  val bucket = "bucket"
  val element_id = "element_id"

  def getElement(params: Params) = Try {
      params
        .keys
        .filter(k => k != bucket && k != element_id)
        .head
    }
    .map(json  => parse(json))
    .map(_.extract[Element])
    .toOption

  def getQuery(params: Params) = Try {
    params
      .keys
      .head
  }
  .map(json => parse(json))
  .map(_.extract[Query])
  .toOption

  // example request
  // curl -g -XPOST localhost:8080/post/user/dc415c6b-5564-40b9-95bb-ebdf0d1560e4 -d '{"section":["3f063265","673a3a2d"],"referrer":["twitter"]}'
  post(s"/post/:$bucket/:$element_id") {
    new AsyncResult {
      val is: Future[String] = {
        val b = params(bucket)
        val e = params(element_id)
        val data = getElement(params)
        val post: Option[Future[MultiBulk]] = data.map(dao.post(b, e))
        if(post.isEmpty) {
          Future("error")
        } else {
          Future.successful("") // TODO be more rigorous
        }
      }
    }
  }

  // example request
  // curl -XPOST localhost:8080/count -d '{"filter":[[{"user":{"referrer":["*"],"section":["*"]}}]],"buckets":["user"],"dimensions":["*"]}'
  post(s"/count") {
    new AsyncResult {
      val is: Future[String] = Future {
        val query: Option[Query] = getQuery(params)
        println(query)
        query.map(dao.getCount)
          .map(r => Serialization.write(r))
          .getOrElse("error")
      }
    }
  }

}