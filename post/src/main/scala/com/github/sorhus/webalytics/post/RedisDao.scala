package com.github.sorhus.webalytics.post

import java.util.UUID

import akka.actor.ActorSystem
import redis.commands.TransactionBuilder
import redis.RedisClient
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class RedisDao(implicit akkaSystem: ActorSystem) extends AudienceDao {

  val redis = RedisClient()
  val r = "_" // reserved char

  private def getRandomKey = UUID.randomUUID().toString

  override def post(bucket: Bucket, element_id: ElementId, element: Element)(implicit metaDao: MetaDao) {
    val document_id: Long = metaDao.getDocumentId(element_id)
    val transaction: TransactionBuilder = redis.transaction()
    metaDao.addMeta(bucket, element)
    element.e.foreach{case(dimension, vals) =>
      vals.foreach{value =>
        transaction.setbit(getKey(bucket, dimension, value), document_id, true)
      }
    }
    transaction.exec()
  }

  def getKey(bucket: Bucket, dimension: Dimension, value: Value) = s"${bucket.b}$r${dimension.d}$r${value.v}"

  private def getAudience(filter: Filter)(implicit metaDao: MetaDao) = {
    val transaction = redis.transaction()
    val ored = filter.f.map{ ands: List[Map[Bucket, Element]] =>
      val destination = getRandomKey
      ands.map{ ors: Map[Bucket, Element] =>
        val keys = ors.flatMap{case(bucket, element) =>
          element.e.flatMap{
            case(dimension, Value("*") :: Nil) =>
              metaDao.getDimensionValues(dimension :: Nil).flatMap(_._2)
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


  private def getCounts(audience: String, dimVals: List[(Dimension, List[Value])], buckets: List[Bucket]): List[(Bucket, List[(Dimension, List[(Value, Future[Long])])])] = {
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

  override def getCount(query: Query)(implicit metaDao: MetaDao): List[(Bucket, List[(Dimension, List[(Value, Long)])])] = {
    val audience = getAudience(query.filter)
    val dimensionValues = metaDao.getDimensionValues(query.dimensions)
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
