package com.github.sorhus.webalytics.batch

import java.io.{BufferedWriter, OutputStreamWriter}
import java.util.UUID

import org.json4s.jackson.Serialization
import org.json4s.{DefaultFormats, Formats}

import scala.collection.mutable
import scala.util.Random

object GenerateData extends App {

  val nElements = args(0).toInt
  val nDimensions = args(1).toInt
  val nValues = args(2).toInt
  val bulkFormat = if(args.length > 3) args(3) == "bulk" else false

  val out = new BufferedWriter(new OutputStreamWriter(System.out))
  val err = new BufferedWriter(new OutputStreamWriter(System.err))

  new GenerateData(nElements, nDimensions, nValues, bulkFormat, out, err).run()
}

class GenerateData(nElements: Int, nDimensions: Int, nValues: Int, bulkFormat: Boolean, out: BufferedWriter, err: BufferedWriter) extends Runnable {

  implicit val jsonFormats: Formats = DefaultFormats

  val values: Map[String, List[String]] = Range(0, nDimensions).map{ _ =>
    UUID.randomUUID().toString -> Range(0, nValues).map(_ => UUID.randomUUID().toString.take(8)).toList
  }.toMap

  def getAsString(data: Map[String, List[String]]) = {
    values.map{case(d,vals) =>
      vals.map(v => if(data(d).contains(v)) v else "_").mkString(",")
    }
  }.mkString("",",","\n")

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

  override def run(): Unit = {
    Range(0,nElements).foreach{ _ =>
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
      if(bulkFormat) {
        out.write(s"$id\t${Serialization.write(data)}\n")
      } else {
        out.write(s"curl -g -XPOST localhost:8080/post/user/$id -d '${Serialization.write(data)}' &\n")
      }
      err.write(getAsString(data))
    }

    out.close()
    err.close()
  }
}