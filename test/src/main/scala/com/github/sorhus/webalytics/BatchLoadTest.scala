package com.github.sorhus.webalytics

import com.github.sorhus.webalytics.model._
import com.github.sorhus.webalytics.client.Webalytics

object BatchLoadTest extends App{

  val w = new Webalytics

  test()

  System.exit(0)

  def test() = {
    val data = generate()

    val sets = "a" :: "b" :: "c" :: Nil

    val filtered = sets.map{ set =>
      val filtered = data.filter(getFilter(set))
      w.batchPost(filtered, set)
      set -> filtered
    }

    Thread.sleep(1000)

    val r = w.getAll()

    filtered.foreach{case(name, content)  =>
      println(s"${r(name)} -> ${content.size}")
      if(r(name)("d")("v") != content.size) {
        throw new RuntimeException("a")
      }
    }

    val a = Map(Bucket("a") -> Element.fromMap(Map("d" -> Set("v"))))
    val b = Map(Bucket("b") -> Element.fromMap(Map("d" -> Set("v"))))
    val c = Map(Bucket("c") -> Element.fromMap(Map("d" -> Set("v"))))

    val aa = filtered.head._2.toSet
    val bb = filtered.tail.head._2.toSet
    val cc = filtered.tail.tail.head._2.toSet

    {
      val aAndB = List(List(a), List(b))
      val query = Query(Filter(aAndB), List(Bucket("a")), List(Dimension("d")))
      val qr = w.get(query)
      val r = aa.intersect(bb).size
      println(s"${qr("a")} -> $r")
      if (qr("a")("d")("v") != r) {
        throw new RuntimeException()
      }
    }

    {
      val aAndC = List(List(a),List(c))
      val query = Query(Filter(aAndC), List(Bucket("a")), List(Dimension("d")))
      val qr = w.get(query)
      val r = aa.intersect(cc).size
      println(s"${qr("a")} -> $r")
      if(qr("a")("d")("v") != r) {
        throw new RuntimeException()
      }
    }

    {
      val aAndBAndC = List(List(a),List(b),List(c))
      val query = Query(Filter(aAndBAndC), List(Bucket("a")), List(Dimension("d")))
      val qr = w.get(query)
      val r = aa.intersect(bb).intersect(cc).size
      println(s"${qr("a")} -> $r")
      if(qr("a")("d")("v") != r) {
        throw new RuntimeException()
      }
    }


//      val aOrB = List(List(a,b))
//      val aOrC = List(List(a,c))
//      val aOrBOrC = List(List(a,b,c))


//      val aaOrBb = aa union bb
//      val aaOrCc = aa union bb
//      val aaOrBbOrCc = aa union bb union cc


    println("test finished successfully")
  }

  def getFilter(bucket: String) = { (t: (ElementId, Element)) =>
    def hash(e: ElementId) = Math.abs(e.e.hashCode)
    val m = 6
    bucket match {
      case "a" => Set(0,3,4,5).contains(hash(t._1) % m)
      case "b" => Set(1,3,5).contains(hash(t._1) % m)
      case "c" => Set(2,4,5).contains(hash(t._1) % m)
      case _ => throw new RuntimeException
    }
  }

  def generate(): Iterable[(ElementId, Element)] = {
    Range(0,1000).map{_ =>
      (ElementId(), Element.fromMap(Map("d" -> Set("v"))))
    }
  }
}
