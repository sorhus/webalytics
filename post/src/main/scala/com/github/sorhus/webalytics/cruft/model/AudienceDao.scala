package com.github.sorhus.webalytics.cruft.model

import com.github.sorhus.webalytics.akka.model._

trait AudienceDao {

  def post(bucket: Bucket, element_id: ElementId, element: Element)(implicit metaDao: MetaDao): Unit
//  def post(bucket: Bucket, points: List[DataPoint])(implicit metaDao: MetaDao): Unit

  def getCount(query: Query)(implicit metaDao: MetaDao): List[(Bucket, List[(Dimension, List[(Value, Long)])])]
}