package com.github.sorhus.webalytics.akka.model

sealed trait Event extends Serializable
case class PostEvent(bucket: Bucket, elementId: ElementId, documentId: DocumentId, element: Element) extends Event
case class PostMetaEvent(bucket: Bucket, element: Element) extends Event
case class QueryEvent(query: Query, space: Element) extends Event
