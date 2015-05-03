import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.cl._
import Array._
import scala.math._
import org.apache.spark.rdd._
import java.net._

class Point(val x: Float, val y: Float, val z: Float)
    extends java.io.Serializable {
  def this() {
    this(0.0f, 0.0f, 0.0f)
  }

  def dist(center : Point) : (Float) = {
    val diffx : Float = center.x - x
    val diffy : Float = center.y - y
    val diffz : Float = center.z - z
    sqrt(diffx * diffx + diffy * diffy + diffz * diffz).asInstanceOf[Float]
  }
}

object SparkKMeans {
    def main(args : Array[String]) {
        if (args.length < 1) {
            println("usage: SparkKMeans cmd")
            return;
        }

        val cmd = args(0)

        if (cmd == "convert") {
            convert(args.slice(1, args.length))
        } else if (cmd == "run") {
            run_kmeans(args.slice(1, args.length))
        }
    }

    def get_spark_context(appName : String) : SparkContext = {
        val conf = new SparkConf()
        conf.setAppName(appName)

        val localhost = InetAddress.getLocalHost
        // val localIpAddress = localhost.getHostAddress
        conf.setMaster("spark://" + localhost.getHostName + ":7077") // 7077 is the default port

        return new SparkContext(conf)
    }

    def run_kmeans(args : Array[String]) {
        if (args.length != 3) {
            println("usage: SparkKMeans run K iters input-path");
            return;
        }
        val sc = get_spark_context("Spark KMeans");

        val K = args(0).toInt;
        val iters = args(1).toInt;
        val inputPath = args(2);

        val raw_points : RDD[Point] = sc.objectFile(inputPath)
        val points : CLWrapperRDD[Point] = CLWrapper.cl[Point](raw_points)
        val samples : Array[Point] = points.takeSample(false, K);

        var centers = new Array[(Int, Point)](K)
        for (i <- samples.indices) {
            val s = samples(i)

            centers(i) = (i, new Point(s.x, s.y, s.z))
        }

        var iter = 0
        while (iter < iters) {
            val classified = points.map(point => {
                var closest_center = -1
                var closest_center_dist = -1.0f

                var i = 0
                while (i < centers.length) {
                    val d = point.dist(centers(i)._2)
                    if (i == 0 || d < closest_center_dist) {
                        closest_center = i
                        closest_center_dist = d
                    }

                    i += 1
                }
                (centers(closest_center)._1, new Point(
                     centers(closest_center)._2.x, centers(closest_center)._2.y,
                     centers(closest_center)._2.z))
            })
            val counts = classified.countByKey()
            val sums = classified.reduceByKey((a, b) => new Point(a.x + b.x,
                    a.y + b.y, a.z + b.z))
            val averages = sums.map(kv => {
                val cluster_index:Int = kv._1;
                val p:Point = kv._2;
                (cluster_index, new Point(p.x / counts(cluster_index),
                    p.y / counts(cluster_index),
                    p.z / counts(cluster_index))) } )

            centers = averages.collect
            println("Iteration " + (iter + 1))
            for (a <- centers) {
                val p:Point = a._2;
                println("  Cluster " + a._1 + ", (" + p.x + ", " + p.y +
                        ", " + p.z + ")")
            }
            iter += 1
        }
    }

    def convert(args : Array[String]) {
        if (args.length != 2) {
            println("usage: SparkKMeans convert input-dir output-dir");
            return
        }
        val sc = get_spark_context("Spark KMeans Converter");

        val inputDir = args(0)
        var outputDir = args(1)
        val input = sc.textFile(inputDir)
        val converted = input.map(line => {
            val tokens = line.split(" ")
            val x = tokens(0).toFloat
            val y = tokens(1).toFloat
            val z = tokens(2).toFloat
            new Point(x, y, z) })
        converted.saveAsObjectFile(outputDir)
    }

}
