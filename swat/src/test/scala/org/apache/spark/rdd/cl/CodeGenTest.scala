package org.apache.spark.rdd.cl

import java.io.IOException
import java.util.LinkedList
import com.amd.aparapi.internal.model.HardCodedClassModels
import com.amd.aparapi.internal.writer.ScalaArrayParameter

trait CodeGenTest[P, R] {

  def getExpectedKernel() : String
  def getExpectedNumInputs() : Int
  def getFunction() : Function1[P, R]
  def init() : HardCodedClassModels
  def complete(params : LinkedList[ScalaArrayParameter])
  def getExpectedException() : String

  def getExpectedKernelHelper(cls : Class[_]) : String = {
    val className : String = cls.getSimpleName
    var hostName : String = java.net.InetAddress.getLocalHost.getHostName
    val tokens : Array[String] = hostName.split('.')
    if (tokens.length > 3) {
      hostName = tokens(tokens.length - 3) + "." + tokens(tokens.length - 2) + "." +
          tokens(tokens.length - 1)
    }

    try {
      scala.io.Source.fromFile(CodeGenTests.testsPath + "/" + hostName + "/" + 
              className.substring(0, className.length - 1) + ".kernel").mkString
    } catch {
      case ioe : IOException => ""
      case e : Exception => throw new RuntimeException(e)
    }
  }

  def shouldEnableNested() : Boolean = {
    true
  }
}
