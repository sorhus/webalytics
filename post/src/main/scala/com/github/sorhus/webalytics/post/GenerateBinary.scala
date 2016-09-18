package com.github.sorhus.webalytics.post

import java.io.{FileReader, BufferedReader, OutputStreamWriter, BufferedWriter}
import akka.actor.ActorSystem
import com.typesafe.config.{ConfigValueFactory, ConfigFactory}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import redis.api.hashes.Hmset
import redis.api.sets.Sadd
import redis.api.strings.Setbit
import scala.collection.mutable

object GenerateBinary extends App {

  implicit val jsonFormats: Formats = DefaultFormats

  val dao = {
    implicit val system = {
      val config = ConfigFactory.load()
        .withValue("akka.loglevel", ConfigValueFactory.fromAnyRef("OFF"))
        .withValue("akka.stdout-loglevel", ConfigValueFactory.fromAnyRef("OFF"))
      ActorSystem("webalytics-generate-binary", config)
    }
    val redis = new Redis
    if(!redis.isEmpty()) {
      println("Redis is not empty, exiting")
      System.exit(1)
    }
    system.shutdown()
    redis
  }

  val file = args(0)
  val bucket = args(1)
  val batchSize = if(args.length > 2) args(2).toInt else 100

  val in = new BufferedReader(new FileReader(file))
  val out = new BufferedWriter(new OutputStreamWriter(System.out))

  val dimensionValues = mutable.Map[String, mutable.Set[String]]()
  val elements = mutable.Map[String, Int]()
  var nDocuments = 0

  Stream.continually(in.readLine())
    .takeWhile(_ != null)
    .zipWithIndex
    .foreach{ case(line, documentId) =>
      val Array(elementId, json) = line.split("\t")
      val element = parse(json).extract[Element]
      nDocuments += 1
      elements.put(elementId, documentId)
      element.flatMap{ case (dimension, values) =>
        if (!dimensionValues.contains(dimension)) {
          dimensionValues.put(dimension, mutable.Set[String]())
        }
        values.foreach(dimensionValues(dimension).add)
        values.map{ value =>
          Setbit(dao.getKey(bucket, dimension, value), documentId, true)
        }
      }.map(_.encodedRequest.decodeString("utf-8"))
      .foreach(out.write)
    }


  {
    redis.api.strings.Set(dao.next_element, nDocuments) ::
    Sadd(dao.dimensions, dimensionValues.keys.toSeq) ::
      Hmset(dao.elements, elements.toMap) ::
      Sadd(dao.buckets, bucket :: Nil) ::
      dimensionValues.flatMap{case(dimension, values) =>
        if(values.nonEmpty)
          Sadd(dao.values(dimension), dimensionValues(dimension).toSeq) :: Nil
        else
          Nil
      }.toList
  }
  .map(_.encodedRequest.decodeString("utf-8"))
  .foreach(out.write)

  in.close()
  out.flush()
  out.close()

}

