package com.github.sorhus.webalytics.post

import java.util.UUID

import akka.actor.ActorSystem
import redis.commands.TransactionBuilder
import redis.RedisClient
import redis.protocol.MultiBulk
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}

class Dao(implicit akkaSystem: ActorSystem) {

  val redis = RedisClient()
  val r = "_" // reserved char
  val buckets = s"${r}buckets$r"
  val dimensions = s"${r}dimensions$r"
  def values(dimension: String) = s"${r}values$r$dimension$r"
  val elements = s"${r}elements$r"
  val next_element = s"${r}next_element$r"

  private def getRandomKey = UUID.randomUUID().toString

  private def getDocumentId(element_id: String): Long = {
    val result: Future[Long] = redis.hget(elements, element_id).flatMap{
      case(Some(document_id)) => Future{
        document_id.utf8String.toLong
      }
      case None => redis.incr(next_element).map{ id: Long =>
        redis.hset(elements, element_id, id)
        id
      }
    }
    Await.result(result, Duration.Inf)
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

  def getKey(bucket: String, dimension: String, value: String) = s"$bucket$r$dimension$r$value"

  private def getAudience(filter: Filter) = {
    val transaction = redis.transaction()
    val ored = filter.map{ ands: List[Map[String, Map[String, List[String]]]] =>
      val destination = getRandomKey
      ands.map{ ors: Map[String, Map[String, List[String]]] =>
        val keys = ors.flatMap{case(bucket, dimvals) =>
          dimvals.flatMap{
            case(dimension, "*" :: Nil) =>
              getDimensionValues(dimension :: Nil).flatMap(_._2)
               .map(value => getKey(bucket, dimension, value))
            case(dimension, values) =>
              values.map(value => getKey(bucket, dimension, value))
          }
        }
        transaction.bitopOR(destination, keys.toSeq:_*)
      }
      destination
    }
    val audience = getRandomKey
    transaction.bitopAND(audience, ored:_*)
    transaction.del(ored:_*)
    transaction.exec()
    audience
  }

  private def getDimensionValues(dimensions: List[String]): List[(String, Seq[String])] = {
    def getValues(dimensions: List[String]) = {
      val transaction = redis.transaction()
      val futures: List[(String, Future[Seq[String]])] = dimensions.map{ dimension =>
        dimension -> transaction.smembers(values(dimension)).map(seq => seq.map(_.utf8String))
      }
      transaction.exec()
      futures.map{case(dimension, values) =>
        dimension -> Await.result(values, Duration.Inf)
      }
    }

    val input = dimensions match {
      case "*" :: Nil => Await.result(redis.smembers(this.dimensions), Duration.Inf).map(_.utf8String).toList
      case _ => dimensions
    }

    getValues(input)
  }

  private def getCounts(audience: String, dimVals: List[(String, Seq[String])], buckets: List[String]): List[(String, List[(String, Seq[(String, Future[Long])])])] = {
    val transaction = redis.transaction()
    val result = buckets.map{ bucket =>
      bucket -> dimVals.map{case(dimension, values) =>
        dimension -> values.map{value =>
          val destination = getRandomKey
          transaction.bitopAND(destination, audience, getKey(bucket, dimension, value))
          val count = transaction.bitcount(destination)
          transaction.del(destination)
          value -> count
        }
      }
    }
    transaction.del(audience)
    transaction.exec()
    result
  }

  // TODO do this with scalaz?
  def getCount(query: Query) = {
    val audience = getAudience(query.filter)
    val dimensionValues = getDimensionValues(query.dimensions)
    val counts = getCounts(audience, dimensionValues, query.buckets)
    counts.map{case(k1,v1) =>
      k1 -> v1.map{case(k2,v2) =>
        k2 -> v2.map{case(k3,v3) =>
          k3 -> Await.result(v3, Duration.Inf)
        }
      }
    }
  }

  def isEmpty: Boolean = Await.result(redis.keys("*"), Duration.Inf).isEmpty
}
