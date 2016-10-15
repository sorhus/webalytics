package com.github.sorhus.webalytics.cruft.batch

import java.io._

object GenerateRaw extends App {

  val bucket = args(0)
  val nElements = args(1).toInt
  val nDimensions = args(2).toInt
  val nValues = args(3).toInt

  val stdOut = new BufferedWriter(new OutputStreamWriter(System.out))
  val stdErr = new BufferedWriter(new OutputStreamWriter(System.err))

  val (pipeIn, pipeOut) = {
    val rawOut = new PipedOutputStream()
    val pipeIn = new BufferedReader(new InputStreamReader(new PipedInputStream(rawOut)))
    val pipeOut = new BufferedWriter(new OutputStreamWriter(rawOut))
    (pipeIn, pipeOut)
  }

  val generate = new GenerateData(nElements, nDimensions, nValues, true, pipeOut, stdErr)
  val convert = new ConvertToRaw(bucket, pipeIn, stdOut)

  new Thread(convert).start()
  new Thread(generate).start()
}
