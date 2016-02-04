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
            println("usage: GenerateInput output-dir n-output-files " + 
                    "n-points-per-file nclusters info-file")
            return;
        }

        val outputDir = args(0)
        val nOutputFiles = args(1).toInt
        val pointsPerFile = args(2).toInt
        val nclusters = args(3).toInt
        val infoFile = args(4)

        val r = new scala.util.Random(1)
        val range = 100.0
        val diffmax = 3.0
        val x_clusters = new Array[Double](nclusters)
        val y_clusters = new Array[Double](nclusters)
        val z_clusters = new Array[Double](nclusters)

        for (i <- 0 until nclusters) {
            x_clusters(i) = r.nextDouble * range
            y_clusters(i) = r.nextDouble * range
            z_clusters(i) = r.nextDouble * range
            System.out.println(x_clusters(i) + " " + y_clusters(i) + " " +
                z_clusters(i))
        }

        for (f <- 0 until nOutputFiles) {
            val writer = new PrintWriter(new File(outputDir + "/input." + f))

            for (p <- 0 until pointsPerFile) {
                val diffx = (r.nextDouble * diffmax) - (diffmax / 2.0)
                val diffy = (r.nextDouble * diffmax) - (diffmax / 2.0)
                val diffz = (r.nextDouble * diffmax) - (diffmax / 2.0)
                val c = p % nclusters
                writer.write((x_clusters(c) + diffx) + " " +
                    (y_clusters(c) + diffy) + " " + (z_clusters(c) + diffz) +
                    "\n")
            }
            writer.close
        }

        val infoWriter = new PrintWriter(new File(infoFile))
        infoWriter.write(nclusters + "\n")
        infoWriter.close
    }
}
