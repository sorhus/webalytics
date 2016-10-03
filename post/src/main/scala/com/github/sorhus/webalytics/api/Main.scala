package com.github.sorhus.webalytics.api

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.DefaultServlet
import org.eclipse.jetty.webapp.WebAppContext
import org.scalatra.servlet.ScalatraListener
import util.Try

object Main extends App {

  val dir = Try(args(0)).toOption
  val port = 9000
  val server = new Server(port)
  val context = new WebAppContext()
  context.setContextPath("/")
  context.setResourceBase("src/main/webapp")
  context.addEventListener(new ScalatraListener)
  context.addServlet(classOf[DefaultServlet], "/")
  dir.map(d => context.setAttribute("bitsets", d))
  server.setHandler(context)
  server.start()
  server.join()
}