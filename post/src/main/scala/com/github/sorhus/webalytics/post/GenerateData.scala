package com.github.sorhus.webalytics.post

import java.util.UUID

import org.json4s.{DefaultFormats, Formats}
import org.json4s.jackson.Serialization

import scala.collection.mutable
import scala.util.Random

object GenerateData extends App {

  val n = args(0).toInt
  val nVal = args(1).toInt
  val format = if(args.length > 2) args(2) else ""

  implicit val jsonFormats: Formats = DefaultFormats
  val dimensions: List[String] = "section" :: "referrer" :: Nil
  val values: Map[String, List[String]] = Map(
    "section" -> Range(0, nVal).map(_ => UUID.randomUUID().toString.take(8)).toList,
    "referrer" -> List("google", "twitter", "facebook")
  )

  val stats = {
    val result = mutable.Map[String, mutable.Map[String, Int]]()
    dimensions.foreach { d =>
      result.put(d, mutable.Map[String, Int]())
      values(d).foreach { v =>
        result(d).put(v, 0)
      }
    }
    result
  }


  Range(0,n).foreach{ i =>
    val id = UUID.randomUUID().toString
    val data: Map[String, List[String]] = dimensions.map{ d =>
      d -> values(d).filter { v =>
        val include = Random.nextBoolean()
        if(include)
          stats(d).put(v, stats(d)(v)+1)
        include
      }
    }.toMap
    if(format == "bulk") {
      println(s"$id\t${Serialization.write(data)}")
    } else {
      println(s"curl -g -XPOST localhost:8080/post/user/$id -d '${Serialization.write(data)}' &")
    }
    data
  }

  System.err.println(stats)
}
