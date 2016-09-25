package com.github.sorhus.webalytics.post

trait AudienceDao {

  def post(bucket: Bucket, element_id: ElementId, element: Element)(implicit metaDao: MetaDao): Unit

  def getCount(query: Query)(implicit metaDao: MetaDao): List[(Bucket, List[(Dimension, List[(Value, Long)])])]
}