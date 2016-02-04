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
import org.apache.spark.rdd.cl.SyncCodeGenTest
import org.apache.spark.rdd.cl.CodeGenTest
import org.apache.spark.rdd.cl.CodeGenTests
import com.amd.aparapi.internal.model.HardCodedClassModels

object ExtensionTest extends SyncCodeGenTest[Int, ExtRet] {
  def getExpectedException() : String = { return null }

  def getExpectedKernel() : String = { getExpectedKernelHelper(getClass) }

  def getExpectedNumInputs() : Int = {
    1
  }

  def init() : HardCodedClassModels = { new HardCodedClassModels() }

  def complete(params : LinkedList[ScalaArrayParameter]) { }

  def getFunction() : Function1[Int, ExtRet] = {
    val max_band_try : Int = 0

    val fpgaExtTasks_leftQs: Array[Byte] = null
    val fpgaExtTasks_leftQlen: Array[Int] = null
    val fpgaExtTasks_leftQoffset: Array[Int] = null
    val fpgaExtTasks_leftRs: Array[Byte] = null
    val fpgaExtTasks_leftRlen: Array[Int] = null
    val fpgaExtTasks_leftRoffset: Array[Int] = null
    val fpgaExtTasks_rightQs: Array[Byte] = null
    val fpgaExtTasks_rightQlen: Array[Int] = null
    val fpgaExtTasks_rightQoffset: Array[Int] = null
    val fpgaExtTasks_rightRs: Array[Byte] = null
    val fpgaExtTasks_rightRlen: Array[Int] = null
    val fpgaExtTasks_rightRoffset: Array[Int] = null
    val fpgaExtTasks_w: Array[Int] = null
    val fpgaExtTasks_mat: Array[Byte] = null // same for each
    val matlen : Int = 0
    val fpgaExtTasks_oDel: Array[Int] = null
    val fpgaExtTasks_eDel: Array[Int] = null
    val fpgaExtTasks_oIns: Array[Int] = null
    val fpgaExtTasks_eIns: Array[Int] = null
    val fpgaExtTasks_penClip5: Array[Int] = null
    val fpgaExtTasks_penClip3: Array[Int] = null
    val fpgaExtTasks_zdrop: Array[Int] = null
    val fpgaExtTasks_h0: Array[Int] = null
    val fpgaExtTasks_regScore: Array[Int] = null
    val fpgaExtTasks_qBeg: Array[Int] = null
    val fpgaExtTasks_idx: Array[Int] = null

    new Function[Int, ExtRet] {
      override def apply(i : Int) : ExtRet = {
        extension(fpgaExtTasks_w(i), fpgaExtTasks_regScore(i),
                fpgaExtTasks_rightQlen(i), fpgaExtTasks_rightQoffset(i), fpgaExtTasks_rightQs,
                fpgaExtTasks_leftQlen(i), fpgaExtTasks_leftQoffset(i), fpgaExtTasks_leftQs,
                fpgaExtTasks_rightRlen(i), fpgaExtTasks_rightRoffset(i), fpgaExtTasks_rightRs,
                fpgaExtTasks_idx(i), fpgaExtTasks_penClip5(i),
                fpgaExtTasks_penClip3(i), fpgaExtTasks_qBeg(i),
                fpgaExtTasks_leftRlen(i), fpgaExtTasks_leftRoffset(i),
                fpgaExtTasks_leftRs, fpgaExtTasks_mat, matlen, fpgaExtTasks_oDel(i),
                fpgaExtTasks_eDel(i), fpgaExtTasks_oIns(i),
                fpgaExtTasks_eIns(i), fpgaExtTasks_zdrop(i), fpgaExtTasks_h0(i),
                max_band_try)
      }

      private def SWExtend(
        qLen: Int, qOffset : Int, query: Array[Byte], tLen: Int, tOffset: Int, target: Array[Byte], m: Int, mat: Array[Byte], matlen : Int,
        oDel: Int, eDel: Int, oIns: Int, eIns: Int, w_i: Int, endBonus: Int, zdrop: Int, h0: Int): Array[Int] =  
      {
        var retArray: Array[Int] = new Array[Int](6)
        var eh_e : Array[Int] = new Array[Int](qLen + 1) // score array
        var eh_h : Array[Int] = new Array[Int](qLen + 1) // score array
        var qp: Array[Byte] = new Array[Byte](qLen * m) // query profile
        var oeDel = oDel + eDel
        var oeIns = oIns + eIns
        var i = 0 
        var j = 0 
        var k = 0
        var w = w_i

        while(i < (qLen + 1)) {
          eh_e(i) = 0
          eh_h(i) = 0
          i += 1
        }

        // generate the query profile
        i = 0
        k = 0
        while(k < m) {
          val p = k * m
          
          j = 0
          while(j < qLen) {
            qp(i) = mat(p + query(qOffset + j))
            i += 1
            j += 1
          }

          k += 1
        }
        
        // fill the first row
        eh_h(0) = h0
        if(h0 > oeIns) eh_h(1) = h0 - oeIns
        else eh_h(1) = 0
        j = 2
        while(j <= qLen && eh_h(j-1) > eIns) {
          eh_h(j) = eh_h(j-1) - eIns
          j += 1
        }

        // adjust $w if it is too large
        k = m * m
        var max = 0
        i = 0
        while (i < matlen) {
          if (mat(i) > max) {
            max = mat(i)
          }
          i += 1
        }

        var maxIns = ((qLen * max + endBonus - oIns).toDouble / eIns + 1.0).toInt
        if(maxIns < 1) maxIns = 1
        if(w > maxIns) w = maxIns  // TODO: is this necessary? (in original C implementation)
        var maxDel = ((qLen * max + endBonus - oDel).toDouble / eDel + 1.0).toInt
        if(maxDel < 1) maxDel = 1
        if(w > maxDel) w = maxDel  // TODO: is this necessary? (in original C implementation)

        // DP loop
        max = h0
        var max_i = -1
        var max_j = -1
        var max_ie = -1
        var gscore = -1
        var max_off = 0
        var beg = 0
        var end = qLen

        var isBreak = false
        i = 0
        while(i < tLen && !isBreak) {
          var t = 0
          var f = 0
          var h1 = 0
          var m = 0
          var mj = -1
          var qPtr = target(tOffset + i) * qLen
          // compute the first column
          h1 = h0 - (oDel + eDel * (i + 1))
          if(h1 < 0) h1 = 0
          // apply the band and the constraint (if provided)
          if (beg < i - w) beg = i - w
          if (end > i + w + 1) end = i + w + 1
          if (end > qLen) end = qLen

          j = beg
          while(j < end) {
            // At the beginning of the loop: eh[j] = { H(i-1,j-1), E(i,j) }, f = F(i,j) and h1 = H(i,j-1)
            // Similar to SSE2-SW, cells are computed in the following order:
            //   H(i,j)   = max{H(i-1,j-1)+S(i,j), E(i,j), F(i,j)}
            //   E(i+1,j) = max{H(i,j)-gapo, E(i,j)} - gape
            //   F(i,j+1) = max{H(i,j)-gapo, F(i,j)} - gape
            var h = eh_h(j)
            var e = eh_e(j)   // get H(i-1,j-1) and E(i-1,j)
            eh_h(j) = h1
            h += qp(qPtr + j)
            if(h < e) h = e
            if(h < f) h = f 
            h1 = h            // save H(i,j) to h1 for the next column
            if(m <= h) { 
              mj = j          // record the position where max score is achieved
              m = h           // m is stored at eh[mj+1]
            }
            t = h - oeDel
            if(t < 0) t = 0
            e -= eDel
            if(e < t) e = t   // computed E(i+1,j)
            eh_e(j) = e       // save E(i+1,j) for the next row
            t = h - oeIns
            if(t < 0) t = 0
            f -= eIns
            if(f < t) f = t
            j += 1
          }
          
          eh_h(end) = h1
          eh_e(end) = 0
          // end == j after the previous loop
          if(j == qLen) {
            if(gscore <= h1) {
              max_ie = i
              gscore = h1
            }
          }

          if(m == 0) 
            isBreak = true
          else {
            if(m > max) {
              max = m
              max_i = i
              max_j = mj

              if(max_off < scala.math.abs(mj - i)) max_off = scala.math.abs(mj - i)
            }
            else if(zdrop > 0) {
              if((i - max_i) > (mj - max_j)) 
                if(max - m - ((i - max_i) - (mj - max_j)) * eDel > zdrop) isBreak = true
              else
                if(max - m - ((mj - max_j) - (i - max_i)) * eIns > zdrop) isBreak = true
            }
            
            // update beg and end for the next round
            if(!isBreak) {
              j = mj
              while(j >= beg && eh_h(j) > 0) {
                j -= 1
              }
              beg = j + 1

              j = mj + 2
              while(j <= end && eh_h(j) > 0) {
                j += 1
              }
              end = j
            }
          }

          //println(i + " " + max_ie + " " + gscore)  // testing

          i += 1
        }

        retArray(0) = max
        retArray(1) = max_j + 1
        retArray(2) = max_i + 1
        retArray(3) = max_ie + 1
        retArray(4) = gscore
        retArray(5) = max_off
        
        retArray
      }


      private def extension(extParam_w : Int, extParam_regScore : Int,
          extParam_rightQlen : Int, extParam_rightQoffset : Int, extParam_rightQs : Array[Byte],
          extParam_leftQlen : Int, extParam_leftQoffset : Int, extParam_leftQs : Array[Byte],
          extParam_rightRlen : Int, extParam_rightRoffset : Int, extParam_rightRs : Array[Byte],
          extParam_idx : Int, extParam_penClip5 : Int, extParam_penClip3 : Int,
          extParam_qBeg : Int, extParam_leftRlen : Int,
          extParam_leftRoffset : Int, extParam_leftRs : Array[Byte],
          extParam_mat : Array[Byte], matlen : Int, extParam_oDel : Int, extParam_eDel : Int,
          extParam_oIns : Int, extParam_elns : Int, extParam_zdrop : Int,
          extParam_h0 : Int, MAX_BAND_TRY : Int): ExtRet = {
        var aw0 = extParam_w
        var aw1 = extParam_w
        var qle = -1
        var tle = -1
        var gtle = -1
        var gscore = -1
        var maxoff = -1
        var i = 0
        var isBreak = false
        var prev: Int = -1
        var regScore: Int = extParam_regScore

        var extRet = new ExtRet
        extRet.qBeg = 0
        extRet.rBeg = 0
        extRet.qEnd = extParam_rightQlen
        extRet.rEnd = 0
        extRet.trueScore = extParam_regScore

        if (extParam_leftQlen > 0) {
          while(i < MAX_BAND_TRY && !isBreak) {
            prev = regScore
            aw0 = extParam_w << i
            val results = SWExtend(extParam_leftQlen, extParam_leftQoffset, extParam_leftQs,
                    extParam_leftRlen, extParam_leftRoffset, extParam_leftRs, 5, extParam_mat, matlen,
                    extParam_oDel, extParam_eDel, extParam_oIns, extParam_elns,
                    aw0, extParam_penClip5, extParam_zdrop, extParam_h0)
            regScore = results(0)
            qle = results(1)
            tle = results(2)
            gtle = results(3)
            gscore = results(4)
            maxoff = results(5)
            if (regScore == prev || ( maxoff < (aw0 >> 1) + (aw0 >> 2) ) ) isBreak = true

            i += 1
          }
          extRet.score = regScore

          // check whether we prefer to reach the end of the query
          // local extension
          if(gscore <= 0 || gscore <= (regScore - extParam_penClip5)) {
            extRet.qBeg = extParam_qBeg - qle
            extRet.rBeg = -tle
            extRet.trueScore = regScore
          } else {
            extRet.qBeg = 0
            extRet.rBeg = -gtle
            extRet.trueScore = gscore
          }
        }

        if (extParam_rightQlen > 0) {
          i = 0
          isBreak = false
          var sc0 = regScore
          while(i < MAX_BAND_TRY && !isBreak) {
            prev = regScore
            aw1 = extParam_w << i
            val results = SWExtend(extParam_rightQlen, extParam_rightQoffset, extParam_rightQs,
                    extParam_rightRlen, extParam_rightRoffset, extParam_rightRs, 5, extParam_mat, matlen,
                    extParam_oDel, extParam_eDel, extParam_oIns, extParam_elns,
                    aw1, extParam_penClip3, extParam_zdrop, sc0)
            regScore = results(0)
            qle = results(1)
            tle = results(2)
            gtle = results(3)
            gscore = results(4)
            maxoff = results(5)
            if(regScore == prev || ( maxoff < (aw1 >> 1) + (aw1 >> 2) ) ) isBreak = true

            i += 1
          }
          extRet.score = regScore

          // check whether we prefer to reach the end of the query
          // local extension
          if(gscore <= 0 || gscore <= (regScore - extParam_penClip3)) {
            extRet.qEnd = qle
            extRet.rEnd = tle
            extRet.trueScore += regScore - sc0
          }
          else {
            extRet.qEnd = extParam_rightQlen 
            extRet.rEnd = gtle
            extRet.trueScore += gscore - sc0
          }
        }
        if (aw0 > aw1) extRet.width = aw0
        else extRet.width = aw1
        extRet.idx = extParam_idx

        extRet
      }
    }
  }

}

class EHType(e_i: Int, h_i: Int) {
  var e: Int = e_i
  var h: Int = h_i
}

class ExtRet() {
  var qBeg: Int = -1
  var rBeg: Long = -1
  var qEnd: Int = -1
  var rEnd: Long = -1
  var score: Int = -1
  var trueScore: Int = -1
  var width: Int = -1
  var idx: Int = -1
}
