package com.github.sorhus.webalytics.batch

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.FileChannel.MapMode
import java.util.UUID

import akka.actor.ActorSystem
import com.github.sorhus.webalytics.impl.{ImmutableRoaringMitmapWrapper, RoaringBitmapWrapper}
import com.github.sorhus.webalytics.impl.redis.RedisMetaDao
import com.github.sorhus.webalytics.model._
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.json4s.{DefaultFormats, Formats}
import org.json4s.jackson.JsonMethods._
import org.roaringbitmap.RoaringBitmap
import org.roaringbitmap.buffer.ImmutableRoaringBitmap

import scala.collection.mutable

object BitsetLoader extends App {

  val inputFile = args(0)
  val bucket = Bucket(args(1))
  val outputDir = args(2)

  val in = new BufferedReader(new FileReader(inputFile))
  val out = new BufferedWriter(new OutputStreamWriter(System.out))
  val audienceDao = new BitsetDao[RoaringBitmap](new RoaringBitmapWrapper().create _)

  implicit val system = {
    val config = ConfigFactory.load()
      .withValue("akka.loglevel", ConfigValueFactory.fromAnyRef("OFF"))
      .withValue("akka.stdout-loglevel", ConfigValueFactory.fromAnyRef("OFF"))
    ActorSystem("webalytics-bitset-loader", config)
  }
  val metaDao = new CachedMetaDao(new RedisMetaDao())

  val loader = new BitsetLoader()
  loader.load(bucket, in, audienceDao)(metaDao)
  system.shutdown()
  loader.write(outputDir, audienceDao)

//  audienceDao.bitsets.foreach{case(bucket,dimensions) =>
//    dimensions.foreach{case(dimension,values) =>
//      values.foreach{case(value, bitset) =>
//        println(s"${bucket.b}:${dimension.d}:${value.v} -> ${bitset.cardinality()}")
//      }
//    }
//  }
//
//
//  val mmapped = loader.read(outputDir)
//  mmapped.foreach{case(bucket,dimensions) =>
//    dimensions.foreach{case(dimension,values) =>
//      values.foreach{case(value, bitset) =>
//        println(s"${bucket.b}:${dimension.d}:${value.v} -> ${bitset.getLongCardinality}")
//      }
//    }
//  }

}


class BitsetLoader() {

  implicit val jsonFormats: Formats = DefaultFormats

  def load(bucket: Bucket, in: BufferedReader, audienceDao: BitsetDao[RoaringBitmap])(implicit metaDao: MetaDao): Unit = {
    Stream.continually(in.readLine())
      .takeWhile(_ != null)
      .map(_.split("\t"))
      .map {
        case Array(elementId, json) =>
          val element = parse(json)
            .extract[Map[String, List[String]]]
            .map { case (dimension: String, values: List[String]) =>
              Dimension(dimension) -> values.map(v => Value(v))
            }
          (ElementId(elementId), Element(element))
        case Array(json) =>
          val element = parse(json)
            .extract[Map[String, List[String]]]
            .map { case (dimension: String, values: List[String]) =>
              Dimension(dimension) -> values.map(v => Value(v))
            }
          (ElementId(UUID.randomUUID().toString), Element(element))
      }
      .foreach { case (elementId, element) =>
        audienceDao.post(bucket, elementId, element)
      }
  }

  def write(path: String, audienceDao: BitsetDao[RoaringBitmap]) = {
    audienceDao.bitsets.foreach{case(bucket, dimvals) =>
      dimvals.foreach{case(dimension, values) =>
        values.foreach{case(value, bitset) =>
          val file = new File(s"$path/${bucket.b}/${dimension.d}/${value.v.replace("/","-")}")
          file.getParentFile.mkdirs()
          file.createNewFile()
          val fos = new FileOutputStream(file)
          val dos = new DataOutputStream(fos)
          bitset.impl().runOptimize()
          bitset.impl().serialize(dos)
          dos.close()
        }
      }
    }
  }

  def read(path: String): Map[Bucket, Map[Dimension, Map[Value, Bitset[ImmutableRoaringBitmap]]]] = {
    val files = mutable.Set[RandomAccessFile]()
    val result = new File(path).list().map { bucket =>
      Bucket(bucket) -> new File(s"$path/$bucket").list().map{ dimension =>
        Dimension(dimension) -> new File(s"$path/$bucket/$dimension").list().map{value =>
          val file = new RandomAccessFile(s"$path/$bucket/$dimension/$value", "r")
          files.add(file)
          val memoryMapped = file.getChannel.map(MapMode.READ_ONLY, 0, file.length())
          Value(value) -> new ImmutableRoaringMitmapWrapper(new ImmutableRoaringBitmap(memoryMapped.slice()))
        }.toMap
      }.toMap
    }.toMap

    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
      override def run(): Unit = {
        files.foreach(_.close())
      }
    }))

    result
  }
}
