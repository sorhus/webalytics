package com.github.sorhus.webalytics.post

import java.io.{OutputStreamWriter, BufferedWriter}
import java.util.UUID
import org.json4s.{DefaultFormats, Formats}
import org.json4s.jackson.Serialization
import scala.collection.immutable.{Iterable, IndexedSeq}
import scala.collection.mutable
import scala.util.Random

object GenerateData extends App {

  val n = args(0).toInt
  val nVal = args(1).toInt
  val format = if(args.length > 2) args(2) else ""
  val out = new BufferedWriter(new OutputStreamWriter(System.out))
  val err = new BufferedWriter(new OutputStreamWriter(System.err))

  implicit val jsonFormats: Formats = DefaultFormats
//  val dimensions: List[String] = "section" :: "referrer" :: Nil
  val values: Map[String, List[String]] = Map(
    "section" -> Range(0, nVal).map(_ => UUID.randomUUID().toString.take(8)).toList,
    "referrer" -> List("google", "twitter", "facebook")
  )

  def getAsString(data: Map[String, List[String]]) = {
    values.map{case(d,vals) =>
      vals.map(v => if(data(d).contains(v)) v else "_").mkString(",")
    }
  }.mkString(",")

  val stats = {
    val result = mutable.Map[String, mutable.Map[String, Int]]()
    values.keys.foreach { d =>
      result.put(d, mutable.Map[String, Int]())
      values(d).foreach { v =>
        result(d).put(v, 0)
      }
    }
    result
  }

  val strings = Range(0,n).map{ i =>
    val id = UUID.randomUUID().toString
    val data: Map[String, List[String]] = values.map{case(d, vals) =>
      d -> vals.filter { v =>
        val include = Random.nextBoolean()
        if(include) {
          stats(d).put(v, stats(d)(v)+1)
        }
        include
      }
    }
    if(format == "bulk") {
      out.write(s"$id\t${Serialization.write(data)}\n")
    } else {
      out.write(s"curl -g -XPOST localhost:8080/post/user/$id -d '${Serialization.write(data)}' &\n")
    }
    getAsString(data) // print this and group+count later if this needs to scale
  }

  out.flush()
  out.close()

  strings.groupBy(identity)
    .map{case(k,v) => s"$k ${v.size}\n"}
    .foreach(err.write)

  err.flush()
  err.close()
}
