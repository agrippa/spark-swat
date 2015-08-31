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
