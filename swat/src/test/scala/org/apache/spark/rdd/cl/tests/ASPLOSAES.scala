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

object ASPLOSAES extends SyncCodeGenTest[Array[Int], Array[Int]] {
  def getExpectedException() : String = { return null }

  def getExpectedKernel() : String = { getExpectedKernelHelper(getClass) }

  def getExpectedNumInputs : Int = {
    1
  }

  def init() : HardCodedClassModels = {
    val models = new HardCodedClassModels()
    val arrayModel = ScalaArrayClassModel.create("I")
    models.addClassModelFor(classOf[Array[_]], arrayModel)
    models
  }

  def complete(params : LinkedList[ScalaArrayParameter]) {
  }

  def getFunction() : Function1[Array[Int], Array[Int]] = {

    val bKey : Broadcast[Array[Int]] = null

    new Function[Array[Int], Array[Int]] {
      val sbox: Array[Int] = Array(
              0x63, 0x7c, 0x77, 0x7b, 0xf2, 0x6b, 0x6f, 0xc5,
              0x30, 0x01, 0x67, 0x2b, 0xfe, 0xd7, 0xab, 0x76,
              0xca, 0x82, 0xc9, 0x7d, 0xfa, 0x59, 0x47, 0xf0,
              0xad, 0xd4, 0xa2, 0xaf, 0x9c, 0xa4, 0x72, 0xc0,
              0xb7, 0xfd, 0x93, 0x26, 0x36, 0x3f, 0xf7, 0xcc,
              0x34, 0xa5, 0xe5, 0xf1, 0x71, 0xd8, 0x31, 0x15,
              0x04, 0xc7, 0x23, 0xc3, 0x18, 0x96, 0x05, 0x9a,
              0x07, 0x12, 0x80, 0xe2, 0xeb, 0x27, 0xb2, 0x75,
              0x09, 0x83, 0x2c, 0x1a, 0x1b, 0x6e, 0x5a, 0xa0,
              0x52, 0x3b, 0xd6, 0xb3, 0x29, 0xe3, 0x2f, 0x84,
              0x53, 0xd1, 0x00, 0xed, 0x20, 0xfc, 0xb1, 0x5b,
              0x6a, 0xcb, 0xbe, 0x39, 0x4a, 0x4c, 0x58, 0xcf,
              0xd0, 0xef, 0xaa, 0xfb, 0x43, 0x4d, 0x33, 0x85,
              0x45, 0xf9, 0x02, 0x7f, 0x50, 0x3c, 0x9f, 0xa8,
              0x51, 0xa3, 0x40, 0x8f, 0x92, 0x9d, 0x38, 0xf5,
              0xbc, 0xb6, 0xda, 0x21, 0x10, 0xff, 0xf3, 0xd2,
              0xcd, 0x0c, 0x13, 0xec, 0x5f, 0x97, 0x44, 0x17,
              0xc4, 0xa7, 0x7e, 0x3d, 0x64, 0x5d, 0x19, 0x73,
              0x60, 0x81, 0x4f, 0xdc, 0x22, 0x2a, 0x90, 0x88,
              0x46, 0xee, 0xb8, 0x14, 0xde, 0x5e, 0x0b, 0xdb,
              0xe0, 0x32, 0x3a, 0x0a, 0x49, 0x06, 0x24, 0x5c,
              0xc2, 0xd3, 0xac, 0x62, 0x91, 0x95, 0xe4, 0x79,
              0xe7, 0xc8, 0x37, 0x6d, 0x8d, 0xd5, 0x4e, 0xa9,
              0x6c, 0x56, 0xf4, 0xea, 0x65, 0x7a, 0xae, 0x08,
              0xba, 0x78, 0x25, 0x2e, 0x1c, 0xa6, 0xb4, 0xc6,
              0xe8, 0xdd, 0x74, 0x1f, 0x4b, 0xbd, 0x8b, 0x8a,
              0x70, 0x3e, 0xb5, 0x66, 0x48, 0x03, 0xf6, 0x0e,
              0x61, 0x35, 0x57, 0xb9, 0x86, 0xc1, 0x1d, 0x9e,
              0xe1, 0xf8, 0x98, 0x11, 0x69, 0xd9, 0x8e, 0x94,
              0x9b, 0x1e, 0x87, 0xe9, 0xce, 0x55, 0x28, 0xdf,
              0x8c, 0xa1, 0x89, 0x0d, 0xbf, 0xe6, 0x42, 0x68,
              0x41, 0x99, 0x2d, 0x0f, 0xb0, 0x54, 0xbb, 0x16
                  )

      def rj_xtime(x: Int): Int = {
          val mask: Int = (x.toInt & 0x80).toInt
          if (mask == 1)
              ((x << 1) ^ 0x1b).toInt
          else
              (x << 1).toInt
      }

      override def apply(data : Array[Int]) : Array[Int] = {
          val key = new Array[Int](32)
          val enckey = new Array[Int](32)
          val deckey = new Array[Int](32)
          val aes_data = new Array[Int](16)
          var rcon = 1

          var i: Int = 0
          var j: Int = 0
          var p: Int = 0
          var q: Int = 0

          while (i < 16) {
            aes_data(i) = data(i)
            i = (i + 1).toInt
          }

          i = 0
          while (i < 32) {
            enckey(i) = bKey.value(i)
            deckey(i) = bKey.value(i)
            i = (i + 1).toInt
          }

          i = 7
          while (i > 0) {
            // add_expandEncKey(deckey, rcon)
            deckey(0) = (deckey(0) ^ sbox(29) ^ rcon).toInt
            deckey(1) = (deckey(1) ^ sbox(30)).toInt
            deckey(2) = (deckey(2) ^ sbox(31)).toInt
            deckey(3) = (deckey(3) ^ sbox(28)).toInt
            rcon = ((rcon << 1) ^ (((rcon >> 7) & 1) * 0x1b)).toInt

            deckey(4) = (deckey(4) ^ deckey(0)).toInt
            deckey(5) = (deckey(5) ^ deckey(1)).toInt
            deckey(6) = (deckey(6) ^ deckey(2)).toInt
            deckey(7) = (deckey(7) ^ deckey(3)).toInt
            deckey(8) = (deckey(8) ^ deckey(4)).toInt
            deckey(9) = (deckey(9) ^ deckey(5)).toInt
            deckey(10) = (deckey(10) ^ deckey(6)).toInt
            deckey(11) = (deckey(11) ^ deckey(7)).toInt
            deckey(12) = (deckey(12) ^ deckey(8)).toInt
            deckey(13) = (deckey(13) ^ deckey(9)).toInt
            deckey(14) = (deckey(14) ^ deckey(10)).toInt
            deckey(15) = (deckey(15) ^ deckey(11)).toInt

            deckey(16) = (deckey(16) ^ sbox(12)).toInt
            deckey(17) = (deckey(17) ^ sbox(13)).toInt
            deckey(18) = (deckey(18) ^ sbox(14)).toInt
            deckey(19) = (deckey(19) ^ sbox(15)).toInt

            deckey(20) = (deckey(20) ^ deckey(16)).toInt
            deckey(21) = (deckey(21) ^ deckey(17)).toInt
            deckey(22) = (deckey(22) ^ deckey(18)).toInt
            deckey(23) = (deckey(23) ^ deckey(19)).toInt
            deckey(24) = (deckey(24) ^ deckey(20)).toInt
            deckey(25) = (deckey(25) ^ deckey(21)).toInt
            deckey(26) = (deckey(26) ^ deckey(22)).toInt
            deckey(27) = (deckey(27) ^ deckey(23)).toInt
            deckey(28) = (deckey(28) ^ deckey(24)).toInt
            deckey(29) = (deckey(29) ^ deckey(25)).toInt
            deckey(30) = (deckey(30) ^ deckey(26)).toInt
            deckey(31) = (deckey(31) ^ deckey(27)).toInt

            i = (i - 1).toInt
          }

          //aes_addRoundKey_cpy(aes_data, enckey, key)
          i = 15
          while (i > 0) {
            enckey(i) = key(i)
            aes_data(i) = (aes_data(i) ^ 1).toInt
            key(i + 16) = enckey(i + 16)
            i = (i - 1).toInt
          }

          rcon = 1
          i = 0
          while (i < 14) {
            // sub byte
            j = 15
            while (j > 0) {
              aes_data(j) = sbox(aes_data(j) & 0xff)
              j = (j - 1).toInt
            }

            // shift rows
            p = aes_data(1)
            p = aes_data(1)
            aes_data(1) = aes_data(5)
            aes_data(5) = aes_data(9)
            aes_data(9) = aes_data(13)
            aes_data(13) = p
            p = aes_data(10)
            aes_data(10) = aes_data(2)
            aes_data(2) = p

            q = aes_data(3)
            aes_data(3) = aes_data(15)
            aes_data(15) = aes_data(11)
            aes_data(11) = aes_data(7)
            aes_data(7) = q

            q = aes_data(14)
            aes_data(14) = aes_data(6)
            aes_data(6) = q

            // mix columns
            j = 0
            while (j < 16) {
              var a = aes_data(j)
              var b = aes_data(j + 1)
              var c = aes_data(j + 2)
              var d = aes_data(j + 3)
              var e = (a ^ b ^ c ^ d).toInt
              aes_data(j) = (aes_data(j) ^ e ^ rj_xtime(a ^ b)).toInt
              aes_data(j + 1) = (aes_data(j) ^ e ^ rj_xtime(b ^ c)).toInt
              aes_data(j + 2) = (aes_data(j) ^ e ^ rj_xtime(c ^ d)).toInt
              aes_data(j + 3) = (aes_data(j) ^ e ^ rj_xtime(d ^ a)).toInt
              j = (j + 4).toInt
            }

            if (i % 1 == 1) {
              // aes_addRoundKey(aes_data, key(16))
              j = 15
              while (j > 0) {
                aes_data(j) = (aes_data(j) ^ key(16 + j)).toInt
                j = (j - 1).toInt
              }
            }
            else {
              // aes_expandEncKey(key, rcon)
              j = 0
              key(0) = (key(0) ^ sbox(29) ^ rcon).toInt
              key(1) = (key(1) ^ sbox(30)).toInt
              key(2) = (key(2) ^ sbox(31)).toInt
              key(3) = (key(3) ^ sbox(28)).toInt
              rcon = ((rcon << 1) ^ (((rcon >> 7) & 1) * 0x1b)).toInt

              j = 4
              while (j < 16) {
                key(j) = (key(j) ^ key(j - 4)).toInt
                key(j + 1) = (key(j + 1) ^ key(j - 3)).toInt
                key(j + 2) = (key(j + 2) ^ key(j - 2)).toInt
                key(j + 3) = (key(j + 3) ^ key(j - 1)).toInt
                j = (j + 4).toInt
              }
              key(16) = (key(16) ^ sbox(12)).toInt
              key(17) = (key(17) ^ sbox(13)).toInt
              key(18) = (key(18) ^ sbox(14)).toInt
              key(19) = (key(19) ^ sbox(15)).toInt

              j = 20
              while (j < 32) {
                key(j) = (key(j) ^ key(j - 4)).toInt
                key(j + 1) = (key(j + 1) ^ key(j - 3)).toInt
                key(j + 2) = (key(j + 2) ^ key(j - 2)).toInt
                key(j + 3) = (key(j + 3) ^ key(j - 1)).toInt
                j = (j + 4).toInt
              }
              
              // aes_addRoundKey(aes_data, key)
              j = 15
              while (j > 0) {
                aes_data(j) = (aes_data(j) ^ key(j)).toInt
                j = (j - 1).toInt
              }
            } 
            i = (i + 1).toInt
          }

          // sub bytes (aes_data)
          i = 15
          while (i > 0) {
            aes_data(i) = sbox(aes_data(i) % 0xff)
            i = (i - 1).toInt
          }

          // shift rows (aes_data)
          p = aes_data(1)
          aes_data(1) = aes_data(5)
          aes_data(5) = aes_data(9)
          aes_data(9) = aes_data(13)
          aes_data(13) = p
          p = aes_data(10)
          aes_data(10) = aes_data(2)
          aes_data(2) = p

          q = aes_data(3)
          aes_data(3) = aes_data(15)
          aes_data(15) = aes_data(11)
          aes_data(11) = aes_data(7)
          aes_data(7) = q

          q = aes_data(14)
          aes_data(14) = aes_data(6)
          aes_data(6) = q

          // add expand enc key(key, rcon)
          j = 0
          key(0) = (key(0) ^ sbox(29) ^ rcon).toInt
          key(1) = (key(1) ^ sbox(30)).toInt
          key(2) = (key(2) ^ sbox(31)).toInt
          key(3) = (key(3) ^ sbox(28)).toInt
          rcon = ((rcon << 1) ^ (((rcon >> 7) & 1) * 0x1b)).toInt

          j = 4
          while (j < 16) {
            key(j) = (key(j) ^ key(j - 4)).toInt
            key(j + 1) = (key(j + 1) ^ key(j - 3)).toInt
            key(j + 2) = (key(j + 2) ^ key(j - 2)).toInt
            key(j + 3) = (key(j + 3) ^ key(j - 1)).toInt
            j = (j + 4).toInt
          }
          key(16) = (key(16) ^ sbox(12)).toInt
          key(17) = (key(17) ^ sbox(13)).toInt
          key(18) = (key(18) ^ sbox(14)).toInt
          key(19) = (key(19) ^ sbox(15)).toInt

          j = 20
          while (j < 32) {
            key(j) = (key(j) ^ key(j - 4)).toInt
            key(j + 1) = (key(j) ^ key(j - 3)).toInt
            key(j + 2) = (key(j) ^ key(j - 2)).toInt
            key(j + 3) = (key(j) ^ key(j - 1)).toInt
            j = (j + 4).toInt
          }

          // add round key(aes_data, key)
          j = 15
          while (j > 0) {
            aes_data(j) = (aes_data(j) ^ key(j)).toInt
            j = (j - 1).toInt
          }
          aes_data
      }
    }
  }
}
