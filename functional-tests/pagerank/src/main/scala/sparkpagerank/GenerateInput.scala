import java.io._

object GenerateInput {
    def main(args : Array[String]) {
        if (args.length != 6) {
            println("usage: GenerateInput output-links-dir " + 
                    "n-output-links-files ndocs mean-nlinks nlinks-range output-docs-file")
            return;
        }

        val outputLinksDir = args(0)
        val nOutputLinksFiles = args(1).toInt
        val nDocs = args(2).toInt
        val meanNLinks = args(3).toInt
        val rangeNLinks = args(4).toInt
        val outputDocsFile = args(5)

        val r = new scala.util.Random(1)

        val docRanks = new Array[Double](nDocs)
        val docLinks = new Array[Int](nDocs)

        var countLinks = 0
        val docsWriter = new PrintWriter(outputDocsFile)
        for (i <- 0 until nDocs) {
            docRanks(i) = r.nextDouble * 100.0
            docLinks(i) = meanNLinks + (r.nextInt(2 * rangeNLinks) - rangeNLinks)
            assert(docLinks(i) > 0)
            countLinks += docLinks(i)

            docsWriter.write(docRanks(i) + " " + docLinks(i) + "\n")
        }
        docsWriter.close()

        val linkSource = new Array[Int](countLinks)
        val linkDest = new Array[Int](countLinks)
        var count = 0
        for (i <- 0 until nDocs) {
            for (j <- 0 until docLinks(i)) {
                linkSource(count) = i
                linkDest(count) = r.nextInt(nDocs)
                count += 1
            }
        }

        val linksPerFile = (countLinks + nOutputLinksFiles - 1) / nOutputLinksFiles
        for (f <- 0 until nOutputLinksFiles) {
            val writer = new PrintWriter(new File(outputLinksDir + "/input." + f))

            val startLink = f * linksPerFile
            var endLink = (f + 1) * linksPerFile
            if (endLink > countLinks) {
                endLink = countLinks
            }

            for (l <- startLink until endLink) {
                writer.write(linkSource(l) + " " + linkDest(l) + "\n")
            }
            writer.close
        }
    }
}
