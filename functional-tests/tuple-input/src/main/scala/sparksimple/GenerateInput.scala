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
        val range = 100.0

        for (f <- 0 until nOutputFiles) {
            val writer = new PrintWriter(new File(outputDir + "/input." + f))

            var uniqueInt : Int = 1
            var uniqueFloat : Float = 1.0f
            for (p <- 0 until pointsPerFile) {
                // val id = r.nextInt(100)
                // val x = r.nextDouble * range
                // val y = r.nextDouble * range
                // val z = r.nextDouble * range
                val id = uniqueInt + 1
                val x = uniqueFloat + 2.0
                val y = uniqueFloat + 3.0
                val z = uniqueFloat + 4.0
                writer.write(id.toString + " " + x.toString + " " + y.toString +
                        " " + z.toString + "\n")

                uniqueFloat = uniqueFloat + 1.0f
                uniqueInt = uniqueInt + 1
            }
            writer.close
        }
    }
}
