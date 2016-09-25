package com.github.sorhus.webalytics.post

import akka.actor.ActorSystem
import redis.RedisClient
import redis.commands.TransactionBuilder

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global

class RedisMetaDao(implicit akkaSystem: ActorSystem) extends MetaDao {

  val r = "_"
  // reserved char
  val elements = s"${r}elements$r"
  val next_element = s"${r}next_element$r"
  val redis: RedisClient = new RedisClient()
  val buckets = s"${r}buckets$r"
  val dimensions = s"${r}dimensions$r"
  def values(dimension: Dimension) = s"${r}values$r${dimension.d}$r"

  override def addMeta(bucket: Bucket, element: Element) = {
    val transaction: TransactionBuilder = redis.transaction()
    element.e.foreach{case(dimension, vals) =>
      transaction.sadd(dimensions, dimension.d)
      vals.foreach{value =>
        transaction.sadd(values(dimension), value.v)
      }
    }
    transaction.exec()
  }

  override def getDocumentId(element_id: ElementId): Long = {
    val result: Future[Long] = redis.hget(elements, element_id.e).flatMap {
      case (Some(document_id)) => Future {
        document_id.utf8String.toLong
      }
      case None => redis.incr(next_element).map { id: Long =>
        redis.hset(elements, element_id.e, id)
        id
      }
    }
    Await.result(result, Duration.Inf)
  }

  override def getDimensionValues(dimensions: List[Dimension]): List[(Dimension, List[Value])] = {
    def getValues(dimensions: List[Dimension]) = {
      val transaction = redis.transaction()
      val futures: List[(Dimension, Future[List[Value]])] = dimensions.map{ dimension =>
        dimension -> transaction.smembers(values(dimension)).map(seq => seq.map(_.utf8String).map(Value.apply).toList)
      }
      transaction.exec()
      futures.map{case(dimension, values) =>
        dimension -> Await.result(values, Duration.Inf)
      }
    }

    val input = dimensions match {
      case Dimension("*") :: Nil =>
        Await.result(redis.smembers(this.dimensions), Duration.Inf)
          .map(_.utf8String)
          .map(Dimension.apply)
          .toList
      case _ => dimensions
    }

    getValues(input)
  }

}
