package com.github.sorhus.webalytics.akka.document

trait GlobalAtomicCounter {
  // TODO
  // if we want to make the app distributed,
  // we'll still need this to coordinate
  // distribution of document ids.
  // impl could be redis
}
