package com.github.sorhus.webalytics.generate

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
    (new BufferedReader(new InputStreamReader(new PipedInputStream(rawOut))),
      new BufferedWriter(new OutputStreamWriter(rawOut)))
  }

  val generate = new GenerateData(nElements, nDimensions, nValues, true, pipeOut, stdErr)
  val convert = new ConvertToRaw(bucket, pipeIn, stdOut)

  new Thread(convert).start()
  new Thread(generate).start()
}
