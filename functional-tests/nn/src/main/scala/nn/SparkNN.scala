import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.cl._
import Array._
import scala.math._
import org.apache.spark.rdd._
import java.net._

import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.Vectors

object SparkNN {
    def main(args : Array[String]) {
        if (args.length < 1) {
            println("usage: SparkNN cmd")
            return;
        }

        val cmd = args(0)

        if (cmd == "convert") {
            convert(args.slice(1, args.length))
        } else if (cmd == "run") {
            run_nn(args.slice(2, args.length), args(1).toBoolean)
        } else if (cmd == "check") {
            // TODO
            val correct = run_nn(args.slice(1, args.length), false)
            val actual = run_nn(args.slice(1, args.length), true)
            System.err.println("PASSED")
        }
    }

    def get_spark_context(appName : String) : SparkContext = {
        val conf = new SparkConf()
        conf.setAppName(appName)

        val localhost = InetAddress.getLocalHost
        conf.setMaster("spark://" + localhost.getHostName + ":7077") // 7077 is the default port

        return new SparkContext(conf)
    }

    // Return the weights and biases of each layer?
    def run_nn(args : Array[String], useSwat : Boolean) :
          Tuple2[Array[DenseVector], Array[DenseVector]] = {
        if (args.length != 3) {
            System.err.println("usage: SparkNN run info-file " +
                    "training-data-path correct-data-path")
            return (new Array[DenseVector](0), new Array[DenseVector](0))
        }
        val infoFilename = args(0)
        val trainingDataPath = args(1)
        val correctDataPath = args(2)
        val sc = get_spark_context("Spark NN");

        var rand : java.util.Random = new java.util.Random(1)

        val infoLines = scala.io.Source.fromFile(infoFilename).getLines()
        val layerDimensionalities : Array[Int] = new Array[Int](infoLines.length)
        var index = 0
        for (line <- infoLines) {
            layerDimensionalities(index) = line.toInt
            index += 1
        }
        /*
         * Represent a L - 1 x M x N matrix where:
         *   L = # of layers
         *   M = # of neurons in layer l
         *   N = # of neurons in layer l - 1
         * The first layer is ignored because it has no inputs, which therefore
         * have no weights.
         */
        val weights : Array[DenseVector] = new Array[DenseVector](
                layerDimensionalities.length - 1)
        for (i <- 0 until weights.length) { // for each layer
            val layerMatrixSize = layerDimensionalities(i + 1) *
                layerDimensionalities(i)
            val arr : Array[Double] = new Array[Double](layerMatrixSize)
            for (j <- 0 until layerMatrixSize) {
                arr(j) = 5.0 * rand.nextDouble
            }

            weights(i) = Vectors.dense(arr).asInstanceOf[DenseVector]
        }

        /*
         * Output biases for all but the first input layer (should the first
         * input layer have an output bias?)
         */
        val biases : Array[DenseVector] = new Array[DenseVector](
                layerDimensionalities.length - 1)
        for (i <- 0 until biases.length) {
            val arr : Array[Double] = new Array[Double](layerDimensionalities(i + 1))
            for (j <- 0 until layerDimensionalities(i + 1)) {
                arr(j) = 5.0 * rand.nextDouble
            }
            biases(i) = Vectors.dense(arr).asInstanceOf[DenseVector]
        }

        /*
         * Element i in raw_y corresponds to the expected output for element i
         * in raw_inputs.
         */
        val raw_inputs = sc.objectFile[DenseVector](trainingDataPath)
        var activations = if (useSwat) CLWrapper.cl[DenseVector](raw_inputs) else raw_inputs
        val raw_y = sc.objectFile[DenseVector](correctDataPath)
        var y = if (useSwat) CLWrapper.cl[DenseVector](raw_y) else raw_y

        val broadcastedWeights = sc.broadcast(weights)
        val broadcastedBiases = sc.broadcast(biases)

        // Feed forward, skip first input layer
        for (l <- 1 until layerDimensionalities.length) {
            val prevLayerSize = layerDimensionalities(l - 1)
            val layerSize = layerDimensionalities(l)
            val new_activations = activations.map(datapoint => {
                  val new_arr : Array[Double] = new Array[Double](layerSize)
                  var i = 0
                  while (i < layerSize) {
                      var acc = 0.0
                      var j = 0
                      // weighted inputs
                      while (j < prevLayerSize) {
                          acc += (broadcastedWeights.value(l - 1)(i * prevLayerSize + j) * datapoint(j))
                          j += 1
                      }

                      acc += broadcastedBiases.value(l - 1)(i) // bias
                      acc = 1.0 / (1.0 + Math.exp(-1.0 * acc)) // sigmoid
                      new_arr(i) = acc
                      i += 1
                  }
                  Vectors.dense(new_arr).asInstanceOf[DenseVector]
                })
            activations = if (useSwat) CLWrapper.cl[DenseVector](new_activations) else new_activations
        }
        // TODO Backward propagation to update weights and biases
        val pairedXY : RDD[Tuple2[DenseVector, DenseVector]] = activations.zip(y)
        val delta : RDD[DenseVector] = pairedXY.map(pair => {
              val size = pair._1.size
              var arr : Array[Double] = new Array[Double](size)
              var i = 0
              while (i < size) {
                arr(i) = pair._1(i) - pair._2(i)
                i += 1
              }
            })
        return (new Array[DenseVector](0), new Array[DenseVector](0))
    }

    def convert(args : Array[String]) {
    }
}
