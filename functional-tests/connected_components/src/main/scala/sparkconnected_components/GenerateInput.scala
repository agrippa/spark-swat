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

import java.io._

object GenerateInput {
    def main(args : Array[String]) {
        if (args.length != 5) {
            println("usage: GenerateInput output-links-dir " +
                "n-output-links-files nnodes nlinks output-info-file")
            return;
        }

        val outputLinksDir = args(0)
        val nOutputLinksFiles = args(1).toInt
        val nNodes = args(2).toInt
        val nLinks = args(3).toInt
        val infoFile = args(4)

        val infoWriter = new PrintWriter(new File(infoFile))
        infoWriter.write(nNodes + "\n")
        infoWriter.write(nLinks + "\n")
        infoWriter.close

        val r = new scala.util.Random(1)

        val linksPerFile = (nLinks + nOutputLinksFiles - 1) / nOutputLinksFiles
        for (f <- 0 until nOutputLinksFiles) {
            val writer = new PrintWriter(new File(outputLinksDir + "/input." + f))

            val startLink = f * linksPerFile
            var endLink = (f + 1) * linksPerFile
            if (endLink > nLinks) {
                endLink = nLinks
            }

            for (l <- startLink until endLink) {
                val a : Int = r.nextInt(nNodes)
                val b : Int = r.nextInt(nNodes)
                writer.write(a + " " + b + "\n")
            }
            writer.close
        }
    }
}
