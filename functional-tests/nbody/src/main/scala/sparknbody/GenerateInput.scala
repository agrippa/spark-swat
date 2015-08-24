import java.io._

object GenerateInput {
    def main(args : Array[String]) {
        if (args.length != 6) {
            println("usage: GenerateInput output-dir n-output-files " +
                "n-points-per-file info-file pairs-output-dir pairs-per-file")
            return;
        }

        val outputDir = args(0)
        val nOutputFiles = args(1).toInt
        val pointsPerFile = args(2).toInt
        val infoFile = args(3)
        val pairsOutputDir = args(4)
        val pairsPerFile = args(5).toInt

        val r = new scala.util.Random(1)
        val range = 100.0

        val infoWriter = new PrintWriter(new File(infoFile))
        infoWriter.write((nOutputFiles * pointsPerFile).toString)
        infoWriter.close

        val npoints = nOutputFiles * pointsPerFile
        for (f <- 0 until nOutputFiles) {
            val writer = new PrintWriter(new File(outputDir + "/input." + f))

            for (p <- 0 until pointsPerFile) {
                val posx = (r.nextDouble * range)
                val posy = (r.nextDouble * range)
                val posz = (r.nextDouble * range)
                val mass = (r.nextDouble * 3.0)

                writer.write(posx + " " + posy + " " + posz + " " + mass + "\n")
            }
            writer.close
        }

        var curr_file = 0
        var count_pairs = 0
        var writer = new PrintWriter(new File(pairsOutputDir + "/input." + curr_file))
        var p1 = 0
        while (p1 <= npoints) {
            var p2 = 0
            while (p2 <= npoints) {
                if (count_pairs > pairsPerFile) {
                    writer.close
                    curr_file += 1
                    writer = new PrintWriter(new File(pairsOutputDir + "/input." + curr_file))
                    count_pairs = 0
                }

                if (p1 != p2) {
                    writer.write(p1 + " " + p2 + "\n")

                    count_pairs += 1
                }
                p2 += 1
            }
            p1 += 1
        }
        writer.close

        // var targetPoint = 0
        // var sourcePoint = 0
        // var f = 0
        // var done = false
        // while (!done) {
        //     val writer = new PrintWriter(new File(pairOutputDir + "/input." + f))
        //     var p = 0

        //     while (!done && p < pairsPerFile) {
        //         System.err.println(targetPoint + " " + sourcePoint)
        //         if (targetPoint != sourcePoint) {
        //             writer.write(targetPoint + " " + sourcePoint + "\n")
        //             p += 1
        //         }

        //         sourcePoint += 1
        //         if (sourcePoint == npoints) {
        //           targetPoint += 1
        //           sourcePoint = 0

        //           if (targetPoint == npoints) {
        //               done = true
        //           }
        //         }
        //     }

        //     writer.close
        //     f += 1
        // }
    }
}
