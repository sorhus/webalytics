package com.github.sorhus.webalytics.akka.document

import com.github.sorhus.webalytics.model.{DocumentId, ElementId}

case class DocumentIds(counter: Long = 0, ids: Map[ElementId, DocumentId] = Map.empty) extends Serializable {

  def get(elementId: ElementId): DocumentId = ids(elementId)

  def update(elementId: ElementId): DocumentIds = {
    if(ids.contains(elementId)) {
      this
    } else {
      val c = counter + 1
      copy(counter = c, ids = ids + (elementId -> DocumentId(c)))
    }
  }

}
