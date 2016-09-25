package com.github.sorhus.webalytics.post

trait MetaDao {

  def addMeta(bucket: Bucket, element: Element):  Unit
  def getDocumentId(element_id: ElementId): Long
  def getDimensionValues(dimensions: List[Dimension]): List[(Dimension, List[Value])]

}
