import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.cl._
import Array._
import scala.math._
import scala.collection.mutable.ListBuffer
import org.apache.spark.rdd._
import java.net._

/*
 * Based on http://www.bindichen.co.uk/post/AI/fuzzy-c-means.html
 */

class Point(val x: Float, val y: Float, val z: Float)
    extends java.io.Serializable {
  def this() {
    this(0.0f, 0.0f, 0.0f)
  }

  def dist(center : Point) : Float = {
    val diffx : Float = center.x - x
    val diffy : Float = center.y - y
    val diffz : Float = center.z - z
    sqrt(diffx * diffx + diffy * diffy + diffz * diffz).asInstanceOf[Float]
  }
}

class PointMembership(val x : Float, val y : Float, val z : Float,
    val membership : Float) extends java.io.Serializable {
  def this() {
    this(0.0f, 0.0f, 0.0f, 0.0f)
  }
}

object SparkFuzzyCMeans {
    def main(args : Array[String]) {
        if (args.length < 1) {
            println("usage: SparkFuzzyCMeans cmd")
            return;
        }

        val cmd = args(0)

        if (cmd == "convert") {
            convert(args.slice(1, args.length))
        } else if (cmd == "run") {
            run_fuzzy_cmeans(args.slice(1, args.length))
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

    def run_fuzzy_cmeans(args : Array[String]) {
        if (args.length != 4) {
            println("usage: SparkFuzzyCMeans run K iters input-path use-swat");
            return;
        }
        val sc = get_spark_context("Spark Fuzzy CMeans");

        val K = args(0).toInt;
        val iters = args(1).toInt;
        val inputPath = args(2);
        val useSwat = args(3).toBoolean

        val m = 2
        val raw_points : RDD[Point] = sc.objectFile(inputPath)
        val collected_points : Array[Point] = raw_points.collect
        val npoints : Int = collected_points.length
        val samples : Array[Point] = raw_points.takeSample(false, K, 1);
        val broadcasted_points = sc.broadcast(collected_points)

        val point_cluster_pairs_raw : RDD[(Int, Point)] =
            // sc.parallelize(0 to npoints - 1, sc.defaultParallelism * 2).flatMap(point_id => {
            sc.parallelize(0 to npoints - 1).flatMap(point_id => {
              var buffer = new ListBuffer[Tuple2[Int, Point]]()
              for (i <- 0 until K) {
                  buffer += new Tuple2[Int, Point](i, broadcasted_points.value(point_id))
              }
              buffer.toList
            })
        point_cluster_pairs_raw.cache

        val point_cluster_pairs : RDD[(Int, Point)] =
            if (useSwat) CLWrapper.cl[(Int, Point)](point_cluster_pairs_raw) else point_cluster_pairs_raw

        System.err.println("Initial centers:")
        var centers : Array[Point] = new Array[Point](K)
        for (i <- samples.indices) {
            val s = samples(i)

            System.err.println("  " + s.x + ", " + s.y + ", " + s.z)
            centers(i) = new Point(s.x, s.y, s.z)
        }

        val startTime = System.currentTimeMillis

        var iter = 0
        while (iter < iters) {
            val iterStartTime = System.currentTimeMillis
            val broadcastedCenters = sc.broadcast(centers)

            val memberships : RDD[(Int, PointMembership)] = point_cluster_pairs.map(pair => {
                  val center : Point = broadcastedCenters.value(pair._1)
                  val point : Point = pair._2

                  val target_dist : Float = point.dist(center)

                  var sum : Float = 0.0f
                  var i = 0
                  while (i < K) {
                      val dist : Float = point.dist(broadcastedCenters.value(i))
                      if (dist != 0.0f) {
                        val ratio : Float = target_dist / dist
                        sum += ratio
                      }

                      i += 1
                  }

                  if (sum == 0.0f) {
                    // because target_dist == 0.0f
                    (pair._1, new PointMembership(point.x, point.y, point.z, 1.0f))
                  } else {
                    val u : Float = 1 / (scala.math.pow(sum, 2 / (m - 1)).asInstanceOf[Float])
                    val u_m : Float = scala.math.pow(u, m).asInstanceOf[Float]
                    (pair._1, new PointMembership(point.x * u_m, point.y * u_m, point.z * u_m, u_m))
                  }
                })

            val updates : RDD[(Int, PointMembership)] = memberships.reduceByKey((p1, p2) => {
                  new PointMembership(p1.x + p2.x, p1.y + p2.y, p1.z + p2.z, p1.membership + p2.membership)
                })

            val new_clusters : RDD[(Int, Point)] = updates.map(input => {
                  val divisor : Float = input._2.membership

                  (input._1, new Point(input._2.x / divisor, input._2.y / divisor,
                        input._2.z / divisor))
                })

            val new_centers_with_ids : Array[(Int, Point)] = new_clusters.collect
            for (c_id <- new_centers_with_ids) {
                centers(c_id._1) = c_id._2
            }

            broadcastedCenters.unpersist(true)

            val iterEndTime = System.currentTimeMillis
            println("iteration " + (iter + 1) + " : " + (iterEndTime - iterStartTime) + " ms")
            // for (i <- centers.indices) {
            //     val p : Point = centers(i)
            //     println("  Cluster " + i + ", (" + p.x + ", " + p.y +
            //             ", " + p.z + ")")
            // }
            iter += 1
        }

        val endTime = System.currentTimeMillis
        System.err.println("Overall time = " + (endTime - startTime))
    }

    def convert(args : Array[String]) {
        if (args.length != 2) {
            println("usage: SparkFuzzyCMeans convert input-dir output-dir");
            return
        }
        val sc = get_spark_context("Spark Fuzzy CMeans Converter");

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
