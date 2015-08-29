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

    def sigmoid(z : Double) : Double = {
      1.0 / (1.0 + Math.exp(-z))
    }

    def inv_sigmoid(a : Double) : Double = {
      -1.0 * Math.log((1.0 / a) - 1.0)
    }

    def sigmoid_prime(z : Double) : Double = {
      sigmoid(z) * (1.0 - sigmoid(z))
    }

    def get_nabla_w(delta : RDD[DenseVector], activation : RDD[DenseVector]) :
        RDD[DenseVector] = {
      val delta_with_activation = delta.zip(activation)
      delta_with_activation.map(d_with_a => {
        val d : DenseVector = d_with_a._1
        val a : DenseVector = d_with_a._2
        val layerSize = d.size
        val prevLayerSize = a.size
        val new_w : Array[Double] = new Array[Double](prevLayerSize * layerSize)

        var i = 0
        while (i < prevLayerSize * layerSize) {
          new_w(i) = 0.0
          i += 1
        }

        i = 0
        while (i < prevLayerSize) {
            var j = 0
            while (j < layerSize) {
                new_w(i * layerSize + j)  += (a(i) * d(j))
                j += 1
            }
            i += 1
        }
        Vectors.dense(new_w).asInstanceOf[DenseVector]
      })
    }

    def reduce_sum(rdd : RDD[DenseVector]) : DenseVector = {
      rdd.reduce(
        (a : DenseVector, b : DenseVector) => {
          val size = a.size
          val combined : Array[Double] = new Array[Double](size)
          var i = 0
          while (i < size) {
            combined(i) = a(i) + b(i)
            i += 1
          }
          Vectors.dense(combined).asInstanceOf[DenseVector]
        })
    }

    // Return the weights and biases of each layer?
    def run_nn(args : Array[String], useSwat : Boolean) :
          Tuple2[Array[DenseVector], Array[DenseVector]] = {
        if (args.length != 4) {
            System.err.println("usage: SparkNN run info-file " +
                    "training-data-path correct-data-path niters")
            return (new Array[DenseVector](0), new Array[DenseVector](0))
        }
        /*
         * infoFilename should have one line for each layer in the neural net,
         * containing a single integer that is the number of neurons in that
         * layer. This includes the input layer and output layer.
         */
        val infoFilename = args(0)
        /*
         * Path to the input training data to use. This should be in object file
         * format and consists of DenseVector inputs, one for each input data
         * point. The dimensionality of these input vectors must equal the value
         * on the first line of infoFilename.
         */
        val trainingDataPath = args(1)
        /*
         * The expected output for each of the input points in trainingDataPath.
         * Also in object file format and containing DenseVectors, the size of
         * each of these vectors should equal the value on the last line of
         * infoFilename.
         */
        val correctDataPath = args(2)
        // Number of iters to train the neural net over
        val iters = args(3).toInt
        val sc = get_spark_context("Spark NN");

        var rand : java.util.Random = new java.util.Random(1)

        val infoLines = scala.io.Source.fromFile(infoFilename).getLines()
        val layerDimensionalities : Array[Int] = new Array[Int](infoLines.length)
        val nlayers = infoLines.length
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
        val weights : Array[DenseVector] = new Array[DenseVector](nlayers - 1)
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
        val biases : Array[DenseVector] = new Array[DenseVector](nlayers - 1)
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
        val zs = new Array[RDD[DenseVector]](nlayers)
        val activations = new Array[RDD[DenseVector]](nlayers)
        activations(0) = if (useSwat) CLWrapper.cl[DenseVector](raw_inputs) else raw_inputs
        val raw_y = sc.objectFile[DenseVector](correctDataPath)
        var y = if (useSwat) CLWrapper.cl[DenseVector](raw_y) else raw_y

        for (iter <- 0 until iters) {
          val broadcastedWeights = sc.broadcast(weights)
          val broadcastedBiases = sc.broadcast(biases)

          // Feed forward, skip first input layer
          for (l <- 1 until nlayers) {
              val prevLayerSize = layerDimensionalities(l - 1)
              val layerSize = layerDimensionalities(l)
              val new_activations = activations(l - 1).map(datapoint => {
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
                        // z is the value of acc here
                        acc = sigmoid(acc)
                        new_arr(i) = acc
                        i += 1
                    }
                    Vectors.dense(new_arr).asInstanceOf[DenseVector]
                  })
              activations(l) = if (useSwat) CLWrapper.cl[DenseVector](new_activations) else new_activations
              // zs(0) will always be empty
              zs(l) = new_activations.map(datapoint => {
                  val new_arr : Array[Double] = new Array[Double](layerSize)
                  var i = 0
                  while (i < layerSize) {
                    new_arr(i) = inv_sigmoid(datapoint(i))
                  }
                  Vectors.dense(new_arr).asInstanceOf[DenseVector]
              })
          }
          // TODO Backward propagation to update weights and biases
          val pairedXY : RDD[Tuple2[DenseVector, DenseVector]] = activations(nlayers - 1).zip(y)

          // L x M where M is the size of layer l
          val nabla_b : Array[RDD[DenseVector]] = new Array[RDD[DenseVector]](nlayers)
          // L x M x N where M is the size of layer l and N is the size of layer l + 1
          val nabla_w : Array[RDD[DenseVector]] = new Array[RDD[DenseVector]](nlayers)

          var delta : RDD[DenseVector] = pairedXY.map(pair => {
                val activation : DenseVector = pair._1
                val y : DenseVector = pair._2
                val size : Int = activation.size

                var arr : Array[Double] = new Array[Double](size)
                var i : Int = 0
                while (i < size) {
                  arr(i) = activation(i) - y(i) // delta
                  i += 1
                }
                Vectors.dense(arr).asInstanceOf[DenseVector]
              })
          nabla_b(nlayers - 1) = delta
          nabla_w(nlayers - 1) = get_nabla_w(delta, activations(nlayers - 2))
          for (l <- 2 until nlayers) {
              val currLayer = nlayers - l + 1
              val prevLayer = currLayer - 1
              val layerSize = layerDimensionalities(currLayer)
              val prevLayerSize = layerDimensionalities(prevLayer)

              // TODO wrap?
              val delta_and_z : RDD[Tuple2[DenseVector, DenseVector]]= delta.zip(zs(prevLayer))
              delta = delta_and_z.map(d_and_z => {
                    val d : DenseVector = d_and_z._1
                    val z : DenseVector = d_and_z._2
                    var prevArr : Array[Double] = new Array[Double](prevLayerSize)
                    var i = 0
                    while (i < prevLayerSize) {
                      prevArr(i) = 0.0
                      i += 1
                    }

                    i = 0
                    while (i < layerSize) {
                      // For each element in delta and each column in weights
                      var acc : Double = 0.0
                      var j = 0
                      while (j < prevLayerSize) {
                        // transposed
                        prevArr(j) +=
                            (broadcastedWeights.value(currLayer)(j * prevLayerSize + i) * d(i))
                      }
                    }

                    i = 0
                    while (i < prevLayerSize) {
                      prevArr(i) = prevArr(i) * sigmoid_prime(z(i))
                      i += 1
                    }

                    Vectors.dense(prevArr).asInstanceOf[DenseVector]
                  })
              nabla_b(nlayers - l) = delta
              nabla_w(nlayers - l) = get_nabla_w(delta, activations(nlayers - l - 1))
              delta = if (useSwat) CLWrapper.cl[DenseVector](delta) else delta
          }

          /*
           * Add all of the elements in nabla_b[:nlayers - 1] to biases and all of
           * the elements in nabla_w[:nlayers - 1] to weights.
           */
          for (l <- 0 until nlayers - 1) {
            val collected_delta_b : DenseVector = reduce_sum(nabla_b(l))
            assert(collected_delta_b.size == biases(l).size)
            val newBiases : Array[Double] = new Array[Double](biases(l).size)
            for (i <- 0 until collected_delta_b.size) {
              newBiases(i) = biases(l)(i) + collected_delta_b(i)
            }
            biases(l) = Vectors.dense(newBiases).asInstanceOf[DenseVector]

            val collected_delta_weights : DenseVector = reduce_sum(nabla_w(l))
            assert(collected_delta_weights.size == weights(l).size)
            val newWeights : Array[Double] = new Array[Double](weights(l).size)
            for (i <- 0 until collected_delta_weights.size) {
              newWeights(i) = weights(l)(i) + collected_delta_weights(i)
            }
            weights(l) = Vectors.dense(newWeights).asInstanceOf[DenseVector]
          }

          broadcastedWeights.unpersist
          broadcastedBiases.unpersist
        }

        return (weights, biases)
    }

    def convert(args : Array[String]) {
    }
}
