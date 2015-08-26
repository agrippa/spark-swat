import java.io._

object GenerateInput {
    def main(args : Array[String]) {
        if (args.length != 3) {
            println("usage: GenerateInput output-dir n-output-files n-vectors-per-file")
            return;
        }

        val outputDir = args(0)
        val nOutputFiles = args(1).toInt
        val pointsPerFile = args(2).toInt

        val r = new scala.util.Random

        for (f <- 0 until nOutputFiles) {
            val writer = new PrintWriter(new File(outputDir + "/input." + f))

            for (p <- 0 until pointsPerFile) {

                writer.write(r.nextInt(100) + "\n")
            }
            writer.close
        }
    }
}
