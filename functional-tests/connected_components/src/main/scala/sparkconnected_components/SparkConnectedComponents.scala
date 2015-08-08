import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.cl._
import Array._
import scala.math._
import org.apache.spark.rdd._
import java.net._
import scala.io.Source

object SparkConnectedComponents {
    def main(args : Array[String]) {
        if (args.length < 1) {
            println("usage: SparkConnectedComponents cmd")
            return;
        }

        val cmd = args(0)

        val sc = get_spark_context("Spark Connected Components");

        if (cmd == "convert") {
            convert(args.slice(1, args.length), sc)
        } else if (cmd == "run") {
            run_connected_components(args.slice(1, args.length), sc)
        } else if (cmd == "run-cl") {
            run_connected_components_cl(args.slice(1, args.length), sc)
        } else if (cmd == "check") {
            val baseline : Array[Int] = run_connected_components(args.slice(1,
                args.length), sc)
            val test : Array[Int] = run_connected_components_cl(args.slice(1,
                args.length), sc)
            assert(baseline.length == test.length)
            for (i <- baseline.indices) {
              if (baseline(i) != test(i)) {
                System.err.println("Different at index " + i + ", expected " +
                    baseline(i) + " but got " + test(i))
                System.exit(1)
              }
            }
        } else {
          System.err.println("Unknown command \"" + cmd + "\"");
          System.exit(1)
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

    def run_connected_components(args : Array[String], sc : SparkContext) : Array[Int] = {
        if (args.length != 2) {
            println("usage: SparkConnectedComponents run input-link-path input-info-path");
            System.exit(1)
        }

        val inputLinksPath = args(0);
        val inputInfoPath = args(1)

        val infoIter : Iterator[String] = Source.fromFile(inputInfoPath).getLines
        val nNodes : Int = infoIter.next.toInt
        val nLinks : Int = infoIter.next.toInt

        val edges : RDD[(Int, Int)] = sc.objectFile(inputLinksPath)

        val membership : Array[Int] = new Array[Int](nNodes)
        for (i <- membership.indices) {
            membership(i) = i
        }

        var done = false
        var iters = 0
        do {
          val broadcastMembership = sc.broadcast(membership)

          val updates : RDD[(Int, Int)] = edges.map(edge => {
                val component_1 = broadcastMembership.value(edge._1)
                val component_2 = broadcastMembership.value(edge._2)
                if (component_1 == component_2) {
                  // Both already the same component
                  (-1, -1)
                } else {
                    if (component_1 < component_2) {
                      (edge._2, component_1) 
                    } else {
                      (edge._1, component_2)
                    }
                }
              })
          val new_classifications : RDD[(Int, Int)] = updates.reduceByKey(
              (cluster1, cluster2) => { if (cluster1 < cluster2) cluster1 else cluster2 })
          val collected_new_classifications : Array[(Int, Int)] = new_classifications.collect

          for (classification <- collected_new_classifications) {
            if (classification._1 != -1) {
              membership(classification._1) = classification._2
            } else {
              assert(classification._2 == -1)
            }
          }

          done = (collected_new_classifications.length == 1)

          broadcastMembership.unpersist(true)

          iters += 1
        } while (!done);

        val allClusters : java.util.Set[java.lang.Integer] =
            new java.util.HashSet[java.lang.Integer]()
        for (cluster <- membership) {
          allClusters.add(cluster)
        }

        System.out.println("# iters = " + iters)
        System.out.println("# clusters = " + allClusters.size())
        membership
    }

    def run_connected_components_cl(args : Array[String], sc : SparkContext) : Array[Int] = {
        if (args.length != 2) {
            println("usage: SparkConnectedComponents run-cl input-link-path input-info-path");
            System.exit(1)
        }

        val inputLinksPath = args(0);
        val inputInfoPath = args(1)

        val infoIter : Iterator[String] = Source.fromFile(inputInfoPath).getLines
        val nNodes : Int = infoIter.next.toInt
        val nLinks : Int = infoIter.next.toInt

        val raw_edges : RDD[(Int, Int)] = sc.objectFile(inputLinksPath)
        val edges : CLWrapperRDD[(Int, Int)] = CLWrapper.cl[(Int, Int)](raw_edges)

        val membership : Array[Int] = new Array[Int](nNodes)
        for (i <- membership.indices) {
            membership(i) = i
        }

        var done = false
        var iters = 0
        do {
          val broadcastMembership = sc.broadcast(membership)
          System.err.println("broadcastMembership has id " + broadcastMembership.id)

          val updates : RDD[(Int, Int)] = edges.map(edge => {
                val component_1 = broadcastMembership.value(edge._1)
                val component_2 = broadcastMembership.value(edge._2)
                if (component_1 == component_2) {
                  // Both already the same component
                  (-1, -1)
                } else {
                    if (component_1 < component_2) {
                      (edge._2, component_1) 
                    } else {
                      (edge._1, component_2)
                    }
                }
              })
          val new_classifications : RDD[(Int, Int)] = updates.reduceByKey(
              (cluster1, cluster2) => { if (cluster1 < cluster2) cluster1 else cluster2 })
          val collected_new_classifications : Array[(Int, Int)] = new_classifications.collect

          for (classification <- collected_new_classifications) {
            if (classification._1 != -1) {
              membership(classification._1) = classification._2
            } else {
              assert(classification._2 == -1)
            }
          }

          done = (collected_new_classifications.length == 1)

          broadcastMembership.unpersist(true)

          iters += 1
        } while (!done);

        val allClusters : java.util.Set[java.lang.Integer] =
            new java.util.HashSet[java.lang.Integer]()
        for (cluster <- membership) {
          allClusters.add(cluster)
        }

        System.out.println("# iters = " + iters)
        System.out.println("# clusters = " + allClusters.size())
        membership
    }

    def convert(args : Array[String], sc : SparkContext) {
        if (args.length != 2) {
            println("usage: SparkConnectedComponents convert input-links-dir " +
                    "output-links-dir");
            return
        }

        val inputLinksDir = args(0)
        var outputLinksDir = args(1)

        val linksInput = sc.textFile(inputLinksDir)
        val converted = linksInput.map(line => {
            val tokens = line.split(" ")
            val a = tokens(0).toInt
            val b = tokens(1).toInt
            (a, b) })
        converted.saveAsObjectFile(outputLinksDir)
    }

}
