/*
Copyright (c) 2016, Rice University

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

1.  Redistributions of source code must retain the above copyright
     notice, this list of conditions and the following disclaimer.
2.  Redistributions in binary form must reproduce the above
     copyright notice, this list of conditions and the following
     disclaimer in the documentation and/or other materials provided
     with the distribution.
3.  Neither the name of Rice University
     nor the names of its contributors may be used to endorse or
     promote products derived from this software without specific
     prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package org.apache.spark.rdd.cl.tests

import java.util.LinkedList

import com.amd.aparapi.internal.writer.ScalaArrayParameter
import com.amd.aparapi.internal.model.Tuple2ClassModel
import com.amd.aparapi.internal.model.ClassModel
import com.amd.aparapi.internal.model.HardCodedClassModels
import com.amd.aparapi.internal.model.DenseVectorClassModel
import com.amd.aparapi.internal.model.ScalaArrayClassModel

import org.apache.spark.rdd.cl.SyncCodeGenTest
import org.apache.spark.rdd.cl.CodeGenTest
import org.apache.spark.rdd.cl.CodeGenTests
import org.apache.spark.rdd.cl.CodeGenUtil
import org.apache.spark.broadcast.Broadcast

import org.apache.spark.rdd.cl.PrimitiveArrayInputBufferWrapperConfig

class OptionData(
    var sptprice: Float, 
    var strike: Float, 
    var rate: Float, 
    var volatility: Float, 
    var otime: Float, 
    var otype: Float) extends java.io.Serializable {
}

object ASPLOSBlackScholes extends SyncCodeGenTest[OptionData, Float] {

  def getExpectedException() : String = { return null }

  def getExpectedKernel() : String = { getExpectedKernelHelper(getClass) }

  def getExpectedNumInputs : Int = {
    1
  }

  def init() : HardCodedClassModels = {
    val models = new HardCodedClassModels()
    models
  }

  def complete(params : LinkedList[ScalaArrayParameter]) {
  }

  def getFunction() : Function1[OptionData, Float] = {

    new Function[OptionData, Float] {
      def CNDF(inputX: Float): Float = {
        var sign: Int = 0

        var outputX: Float = 0
        var xInput: Float = 0
        var xNPrimeofX: Float = 0
        var expValues: Float = 0
        var xK2: Float = 0
        var xK2_2: Float = 0
        var xK2_3: Float = 0
        var xK2_4: Float = 0
        var xK2_5: Float = 0
        var xLocal: Float = 0
        var xLocal_1: Float = 0
        var xLocal_2: Float = 0
        var xLocal_3: Float = 0

        // Check for negative value of inputX
        if (inputX < 0.0f) {
            xInput = -inputX
            sign = 1
        } else {
            xInput = inputX
            sign = 0
        }

        // Compute NPrimeX term common to both four & six decimal accuracy calcs
        expValues = math.exp(-0.5f * inputX * inputX).toFloat
        xNPrimeofX = expValues
        xNPrimeofX = xNPrimeofX * 0.39894228040143270286f

        xK2 = 0.2316419f * xInput
        xK2 = 1.0f + xK2
        xK2 = 1.0f / xK2
        xK2_2 = xK2 * xK2
        xK2_3 = xK2_2 * xK2
        xK2_4 = xK2_3 * xK2
        xK2_5 = xK2_4 * xK2

        xLocal_1 = xK2 * 0.319381530f
        xLocal_2 = xK2_2 * (-0.356563782f)
        xLocal_3 = xK2_3 * 1.781477937f
        xLocal_2 = xLocal_2 + xLocal_3
        xLocal_3 = xK2_4 * (-1.821255978f)
        xLocal_2 = xLocal_2 + xLocal_3
        xLocal_3 = xK2_5 * 1.330274429f
        xLocal_2 = xLocal_2 + xLocal_3

        xLocal_1 = xLocal_2 + xLocal_1
        xLocal   = xLocal_1 * xNPrimeofX
        xLocal   = 1.0f - xLocal

        outputX = if (sign == 1) 1.0f - xLocal else xLocal
        outputX
      }

      override def apply(in : OptionData) : Float = {
          var optionPrice: Float = 0

          // local private working variables for the calculation
          var xStockPrice: Float = 0
          var xStrikePrice: Float = 0
          var xRiskFreeRate: Float = 0
          var xVolatility: Float = 0
          var xTime: Float = 0
          var xSqrtTime: Float = 0

          var logValues: Float = 0
          var xLogTerm: Float = 0
          var xPowerTerm: Float = 0
          var xD1: Float = 0
          var xD2: Float = 0
          var xPowerTer: Float = 0
          var xDen: Float = 0
          var d1: Float = 0
          var d2: Float = 0
          var futureValueX: Float = 0
          var nofXd1: Float = 0
          var nofXd2: Float = 0
          var negnofXd1: Float = 0
          var negnofXd2: Float = 0

          xStockPrice = in.sptprice
          xStrikePrice = in.strike
          xRiskFreeRate = in.rate
          xVolatility = in.volatility

          xTime = in.otime;
          xSqrtTime = math.sqrt(xTime).toFloat
          logValues = in.sptprice / in.strike
          xLogTerm = logValues

          xPowerTerm = xVolatility * xVolatility
          xPowerTerm = xPowerTerm * 0.5f

          xD1 = xRiskFreeRate + xPowerTerm
          xD1 = xD1 * xTime
          xD1 = xD1 + xLogTerm

          xDen = xVolatility * xSqrtTime
          xD1 = xD1 / xDen
          xD2 = xD1 - xDen

          d1 = xD1
          d2 = xD2

          nofXd1 = CNDF(d1)
          nofXd2 = CNDF(d2)

          futureValueX = xStrikePrice * math.exp(-(xRiskFreeRate)*(xTime)).toFloat
          if (in.otype == 0) {
              optionPrice = (xStockPrice * nofXd1) - (futureValueX * nofXd2)
          } else { 
              negnofXd1 = (1.0f - nofXd1)
              negnofXd2 = (1.0f - nofXd2)
              optionPrice = (futureValueX * negnofXd2) - (xStockPrice * negnofXd1)
          }
          optionPrice
      }
    }
  }
}
