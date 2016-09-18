package com.github.sorhus.webalytics.post

import akka.actor.ActorSystem
import redis.commands.TransactionBuilder
import redis.RedisClient
import redis.protocol.MultiBulk
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.util.Random

class Redis(implicit akkaSystem: ActorSystem) {

  val redis = RedisClient()
  val r = "_" // reserved char
  val buckets = s"${r}buckets$r"
  val elements = s"${r}elements$r"
  val dimensions = s"${r}dimensions$r"
  def values(dimension: String) = s"${r}values$r$dimension$r"
  val next_element = s"${r}next_element$r"

  def getRandomKey = Random.nextString(32) // TODO reimplement


  def getDocumentId(element_id: String): Long = {
    val result: Future[Long] = redis.hget(elements, element_id).flatMap{
      case(Some(document_id)) => Future{
        document_id.utf8String.toLong
      }
      case None => redis.incr(next_element).map{ id: Long =>
        redis.hset(elements, element_id, id)
        id
      }
    }
    Await.result(result, 1.second)
  }

  def post(bucket: String, element_id: String)(data: Map[String,List[String]]): Future[MultiBulk] = {
    val document_id: Long = getDocumentId(element_id)
    val transaction: TransactionBuilder = redis.transaction()
    transaction.sadd(buckets, bucket)
    data.foreach{case(dimension, vals) =>
      transaction.sadd(dimensions, dimension)
      vals.foreach{value =>
        transaction.sadd(values(dimension), value)
        transaction.setbit(getKey(bucket, dimension, value), document_id, true)
      }
    }
    transaction.exec()
  }

  def getKey(bucket: String, dimension: String, value: String) = {
    s"$bucket$r$dimension$r$value"
  }

  def getUniques(query: Query) = {
    val transaction = redis.transaction()
    val ored = query.filter.map{ ands: List[Map[String, Map[String, String]]] =>
      val destination = getRandomKey
      ands.map{ ors: Map[String, Map[String, String]] =>
        val keys = ors.flatMap{case(bucket, dimvals) =>
          dimvals.map{case(dimension, value) =>
            getKey(bucket, dimension, value)
          }
        }
        transaction.bitopOR(destination, keys.toSeq:_*)
      }
      destination
    }
    val audience = getRandomKey
    transaction.bitopAND(audience, ored:_*)
    transaction.exec()

    val dimvals: List[(String, Seq[String])] = query.dimensions.map(dimension => dimension -> Await.result(redis.smembers(values(dimension)), 1.second).map(_.utf8String))


    val result: Map[String, Map[String, Map[String, Long]]] = query.buckets.map{ bucket =>
      bucket -> dimvals.map{case(dimension, vals) =>
        dimension -> vals.map{value =>
          val destination = getRandomKey
          redis.bitopAND(destination, audience, getKey(bucket, dimension, value))
          value -> Await.result(redis.bitcount(destination), 1.second)
        }.toMap
      }.toMap
    }.toMap

    result
  }
}
