import java.io._

object GenerateInput {
    def main(args : Array[String]) {
        if (args.length != 3) {
            println("usage: GenerateInput output-dir n-output-files n-points-per-file")
            return;
        }

        val outputDir = args(0)
        val nOutputFiles = args(1).toInt
        val pointsPerFile = args(2).toInt
        val r = new scala.util.Random
        val range = 100

        for (f <- 0 until nOutputFiles) {
            val writer = new PrintWriter(new File(outputDir + "/input." + f))

            for (p <- 0 until pointsPerFile) {
                val vectorLength = r.nextInt(10) + 1
                for (i <- 0 until vectorLength) {
                  writer.write(r.nextInt(range) + " ")
                }
                writer.write("\n")
            }
            writer.close
        }
    }
}
