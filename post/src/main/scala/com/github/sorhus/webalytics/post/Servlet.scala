package com.github.sorhus.webalytics.post

import akka.actor.ActorSystem
import org.json4s.jackson.Serialization
import org.scalatra.{Params, AsyncResult, FutureSupport, ScalatraServlet}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import redis.protocol.MultiBulk

import scala.concurrent.{Future, ExecutionContext}
import scala.util.Try

case class Query(`type`: String, filter: List[List[Map[String, Map[String,String]]]], buckets: List[String], dimensions: List[String])

class Servlet(implicit system: ActorSystem) extends ScalatraServlet with FutureSupport {

  val redis = new Redis()

  override protected implicit def executor: ExecutionContext = system.dispatcher
  protected implicit val jsonFormats: Formats = DefaultFormats

  val bucket = "bucket"
  val element_id = "element_id"

  def getData(params: Params) = Try {
      params
        .keys
        .filter(k => k != bucket && k != element_id)
        .head
    }
    .map(json  => parse(json))
    .map(_.extract[Map[String,List[String]]])
    .toOption

  def getQuery(params: Params) = Try {
    params
      .keys
      .head
  }
  .map(json => parse(json))
  .map(_.extract[Query])
  .toOption

  post(s"/post/:$bucket/:$element_id") {
    new AsyncResult {
      val is: Future[String] = {
        val b = params(bucket)
        val e = params(element_id)
        val data = getData(params)
        println(data)
        val post: Option[Future[MultiBulk]] = data.map(redis.post(b, e))
        if(post.isEmpty) {
          Future("error\n")
        } else {
          Future("ok\n")
//          post.get.map(_.toByteString.utf8String)
        }
      }
    }
  }

  post(s"/count") {
    new AsyncResult {
      val is: Future[String] = Future {
        val query = getQuery(params)
        println(query)
        query.map(redis.getUniques)
          .map(r => Serialization.write(r))
          .getOrElse("error")
      }
    }
  }

}