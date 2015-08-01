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
