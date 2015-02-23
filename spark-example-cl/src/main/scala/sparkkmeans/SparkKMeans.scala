import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.cl._
import Array._
import scala.math._
import org.apache.spark.rdd._
import java.net._

class Point(x:Double, y:Double, z:Double) extends java.io.Externalizable {
    private var _x = x
    private var _y = y
    private var _z = z

    def this() = this(0.0, 0.0, 0.0)

    def get_x : Double = return _x
    def get_y : Double = return _y
    def get_z : Double = return _z

    def readExternal(in:java.io.ObjectInput) {
        _x = in.readDouble
        _y = in.readDouble
        _z = in.readDouble
    }

    def writeExternal(out:java.io.ObjectOutput) {
        out.writeDouble(_x)
        out.writeDouble(_y)
        out.writeDouble(_z)
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

            centers(i) = (i, s)
        }

        for (iter <- 0 until iters) {
            val classified = points.map(point => classify(point, centers))
            val counts = classified.countByKey()
            val sums = classified.reduceByKey((a, b) => new Point(a.get_x + b.get_x,
                    a.get_y + b.get_y, a.get_z + b.get_z))
            val averages = sums.map(kv => {
                val cluster_index:Int = kv._1;
                val p:Point = kv._2;
                (cluster_index, new Point(p.get_x / counts(cluster_index),
                    p.get_y / counts(cluster_index),
                    p.get_z / counts(cluster_index))) } )

            centers = averages.collect
            println("Iteration " + (iter + 1))
            for (a <- centers) {
                val p:Point = a._2;
                println("  Cluster " + a._1 + ", (" + p.get_x + ", " + p.get_y +
                        ", " + p.get_z + ")")
            }
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
            val x = tokens(0).toDouble
            val y = tokens(1).toDouble
            val z = tokens(2).toDouble
            new Point(x, y, z) })
        converted.saveAsObjectFile(outputDir)
    }

    def classify(point : Point, centers : Array[(Int, Point)]) : (Int, Point) = {
        val x = point.get_x
        val y = point.get_y
        val z = point.get_z

        var closest_center = -1
        var closest_center_dist = -1.0

        for (c <- centers) {
            val center = c._2
            val diffx = center.get_x - x
            val diffy = center.get_y - y
            val diffz = center.get_z - z
            val dist = sqrt(pow(diffx, 2) + pow(diffy, 2) + pow(diffz, 2))

            if (closest_center == -1 || dist < closest_center_dist) {
                closest_center = c._1
                closest_center_dist = dist
            }
        }

        return (closest_center, new Point(x, y, z))
    }
}
