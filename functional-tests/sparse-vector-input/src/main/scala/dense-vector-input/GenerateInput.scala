import java.io._

object GenerateInput {
    def main(args : Array[String]) {
        if (args.length != 5) {
            println("usage: GenerateInput output-dir n-output-files n-vectors-per-file avg-vec-length vec-length-range")
            return;
        }

        val outputDir = args(0)
        val nOutputFiles = args(1).toInt
        val pointsPerFile = args(2).toInt
        val avgLength = args(3).toInt
        val lengthRange = args(4).toInt
        assert(lengthRange < avgLength)

        val r = new scala.util.Random
        val range = 100.0
        val indexRange = 50

        for (f <- 0 until nOutputFiles) {
            val writer = new PrintWriter(new File(outputDir + "/input." + f))

            for (p <- 0 until pointsPerFile) {

                var length = r.nextInt(lengthRange * 2)
                length = length - lengthRange
                length = length + avgLength
                assert(length > 0)

                for (i <- 0 until length) {
                    writer.write(r.nextInt(indexRange) + " " +
                            (r.nextDouble * range) + " ")
                }
                writer.write("\n")
            }
            writer.close
        }
    }
}
