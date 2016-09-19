package com.github.sorhus.webalytics

package object post {
  type Filter = List[List[Map[String, Map[String,List[String]]]]]
  type Element = Map[String,List[String]]
  case class Query(filter: Filter, buckets: List[String], dimensions: List[String])
}
