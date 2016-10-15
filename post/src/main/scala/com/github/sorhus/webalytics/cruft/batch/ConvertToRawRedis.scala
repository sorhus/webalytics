package com.github.sorhus.webalytics.cruft.batch

import java.io.{BufferedReader, BufferedWriter, FileReader, OutputStreamWriter}

import akka.actor.ActorSystem
import com.github.sorhus.webalytics.akka.model.{Bucket, Dimension, Element}
import com.github.sorhus.webalytics.cruft.redis.{RedisDao, RedisMetaDao}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import redis.api.hashes.Hmset
import redis.api.sets.Sadd
import redis.api.strings.Setbit

import scala.collection.mutable

object ConvertToRaw extends App {
  val file = args(0)
  val bucket = args(1)
  val in = new BufferedReader(new FileReader(file))
  val out = new BufferedWriter(new OutputStreamWriter(System.out))
  new ConvertToRaw(bucket, in, out).run()
}

class ConvertToRaw(bucket: String, in: BufferedReader, out: BufferedWriter) extends Runnable {

  implicit val jsonFormats: Formats = DefaultFormats

  val (dao, metaDao) = {
    implicit val system = {
      val config = ConfigFactory.load()
        .withValue("akka.loglevel", ConfigValueFactory.fromAnyRef("OFF"))
        .withValue("akka.stdout-loglevel", ConfigValueFactory.fromAnyRef("OFF"))
      ActorSystem("webalytics-generate-binary", config)
    }
    val dao = new RedisDao()
    val metaDao = new RedisMetaDao()
    system.shutdown()
    (dao, metaDao)
  }

  val dimensionValues = mutable.Map[String, mutable.Set[String]]()
  val elements = mutable.Map[String, Int]()
  var nDocuments = 0

  override def run(): Unit = {
    Stream.continually(in.readLine())
      .takeWhile(_ != null)
      .zipWithIndex
      .foreach { case (line, documentId) =>
        val Array(elementId, json) = line.split("\t")
        val element = parse(json).extract[Element]
        nDocuments += 1
        elements.put(elementId, documentId)
        element.e.flatMap { case (dimension, values) =>
          if (!dimensionValues.contains(dimension.d)) {
            dimensionValues.put(dimension.d, mutable.Set[String]())
          }
          values.map(_.v).foreach(dimensionValues(dimension.d).add)
          values.map{ value =>
            Setbit(dao.getKey(Bucket(bucket), dimension, value), documentId, true)
          }
        }.map(_.encodedRequest.decodeString("utf-8"))
          .foreach(out.write)
      }

    {
      redis.api.strings.Set(metaDao.next_element, nDocuments) ::
        Sadd(metaDao.dimensions, dimensionValues.keys.toSeq) ::
        Sadd(metaDao.buckets, bucket :: Nil) ::
        elements.grouped(1000).map{ group =>
          Hmset(metaDao.elements, group.toMap)
        }.toList :::
        dimensionValues.flatMap{case(dimension, values) =>
          if(values.nonEmpty) {
            Sadd(metaDao.values(Dimension(dimension)), values.toSeq) :: Nil
          } else {
            Nil
          }
        }.toList
    }
      .map(cmd => cmd.encodedRequest.decodeString("utf-8"))
      .foreach(out.write)

    in.close()
    out.close()
  }
}
