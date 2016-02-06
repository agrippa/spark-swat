# Spark SWAT (Spark With Accelerated Tasks)

SWAT accelerates user-defined Spark computation using OpenCL accelerators.

# Overview

SWAT takes Apache Spark programs like the one below:

    val rdd = sc.objectFile(inputPath)
    val nextRdd = rdd.map(i => 2 * i)

and accelerates their user-defined kernels, in this case `i => 2 * i`, by
offloading those kernels to an OpenCL accelerator. For the code snippet above,
performing this offload is as simple as wrapping the input RDD into a SWAT RDD:

    val rdd = CLWrapper.cl(sc.objectFile(inputPath))

Modern-day machine learning is advancing too quickly for fixed-function
accelerated data analytics frameworks like Caffe, BIDMach, and HeteroSpark to
keep up. Domain experts want the best performance for the novel implementation
they wrote yesterday so that they can rapidly iterate, they don't want to wait
six months for a performance expert to get around to tuning it. They also don't
want to muck around with low-level APIs and programming languages like C/C++,
CUDA, or OpenCL.

SWAT supports acceleration of entirely user-defined kernels written as Scala
lambdas and passed to Spark transformations. It aims to maintain an API that is
nearly identical to Apache Spark, minimizing code rewrite and ramp-up time.

Under the hood, an event-driven resource management and scheduling runtime takes
care of the details of mapping the user-defined work down to the current
hardware platform. Automatic kernel generation from JVM bytecode at runtime
enables auto-optimization of user kernels and ensures identical semantics to the
original JVM kernel.

A brief slide deck on SWAT is available under spark-swat/docs/.

# Components

There are 5 main software modules that make up SWAT.

1. **clutil**: A small library of basic OpenCL utility functions used to help manage OpenCL devices.
2. **clalloc**: A thread-safe, single-accelerator memory management library built on
   top of the OpenCL APIs. It exposes two data structures: 1) an allocator
   object for each OpenCL device in a platform that serves as a context/handle
   for clAlloc operations on that device, and 2) region objects which represent
   a contiguous block of allocated memory on a single OpenCL device.
3. **swat-bridge**: A native library that sits on top of clutil and clalloc but
   below the JVM. This is the main interface between components running in the
   JVM and those in native land.
4. **swat**: The core JVM library that handles coordination of data
   serialization, deserialization, code generation, and kernel offload to
   swat-bridge.
5. **aparapi-swat**: The code generation framework we use to convert JVM
   bytecode at runtime to OpenCL kernels. aparapi-swat is an extension of the
   open source APARAPI project and resides in a separate repo, at
   https://github.com/agrippa/aparapi-swat.

# FAQs

1. **Is SWAT production-ready?** No, SWAT is a one-man project at the moment and
   is not being used in production anywhere. With that said, it does have fairly
   comprehensive code generation and functional tests. If you are interested in
   exploring the use of SWAT in your development or contributing to SWAT,
   contact me at jmaxg3@gmail.com.
2. **Are there limitations to the kernels that SWAT can run on an accelerator?**
   Yes, generating OpenCL code from JVM bytecode is no simple task. There are
   many constructs in the JVM that just don't have an analog in OpenCL (e.g.
   exceptions). While it is usually possible to loosen the restrictions on the
   bytecode that can be handled, that loosening sometimes comes with a
   performance penalty from either more expensive code generation or less
   well-performing kernels. With that said, the restrictions on SWAT kernels are
   much looser than most related projects. For example, it can handle some JVM objects (including
   MLlib objects like DenseVector and SparseVector) as well as dynamic memory
   allocation (i.e. `new`). For examples of the types of kernels SWAT
   supports, see the files in spark-swat/swat/src/test/scala/org/apache/spark/rdd/cl/tests.
3. **Will SWAT make my program faster?** This is a tough question to answer and
   is very dependent on the kernel you are running and the data it is
   processing. SWAT (and accelerators in general) do not work well for kernels
   that process a large amount of data without performing many operations on it.
   For example, a simplistic PageRank benchmark does not perform well on SWAT
   but a KMeans kernel may. The image [here](https://github.com/agrippa/spark-swat/raw/master/docs/speedup.png) shows some sample results we have
   gathered on a variety of benchmarks. Note the bimodal distribution: there is
   a cluster of benchmarks that achieve 3-4x speedup on SWAT and another that
   sees no benefit (or slight degradation). Your mileage may vary.
4. **Does SWAT support multi-GPU systems?** Yes!
5. **Do I need to be a accelerator guru to program SWAT effectively?** No, but as in all
   things performance-oriented an understanding of the underlying platform will
   help. In general, the functional/parallel patterns of Spark encourage code
   that will run well on accelerators and the SWAT runtime is also able to
   auto-optimize some things.
6. **What platforms is SWAT tested on?** As a small project, SWAT is currently
   only regularly tested on HotSpot JDK 1.7.8\_80, Spark 1.5.1, Hadoop 2.5.2, GCC 4.8.5,
   CUDA 6.5, NVIDIA GPUs, all under Red Hat Enterprise Linux Server release 6.5.
   It has been used on other systems, including AMD-based clusters, but is not tested regularly there.
   It is likely to run on Linux-based systems, but may need tweaking.

# Setup

There are no automated configuration or installation scripts for SWAT at the
moment as I have no need to support it on more than a couple of platforms, so
building may be a pain. If you are having trouble or hitting odd errors, you can
reach me at jmaxg3@gmail.com and I'll be happy to help.

I'll assume that you already have a Spark cluster deployed and environment
variables like `SPARK_HOME`, `HADOOP_HOME`, and `JAVA_HOME` set
appropriately. `CL_HOME` should point to the root directory of your OpenCL
installation if you are not on OS X. You should also set `SWAT_HOME` to point to the spark-swat
directory you clone this repo to.

1. Check out APARAPI-SWAT from its repo and build:
  1. `git clone https://github.com/agrippa/aparapi-swat`
  2. Set an `APARAPI_SWAT` environment variable to point to the aparapi-swat
     directory that was just created.
  3. `cd $APARAPI_SWAT && ./build.sh`
2. Create a `build.conf` file in `$SWAT_HOME` with a single line setting `GXX=...` to your preferred C++ compiler.
3. `cd $SWAT_HOME/clutil/ && make`
4. `cd $SWAT_HOME/clalloc/ && make`
5. `cd $SWAT_HOME/swat-bridge/ && make`
6. `cd $SWAT_HOME/swat/ && mvn clean package`

To test the code generator is working, use the script in
`$SWAT_HOME/swat/test_translator.sh` to run the code generation
tests.  Before running this program make sure SCALA_HOME is 

Functional tests can be found in `$SWAT_HOME/functional-tests`. See the
`test_all.sh` script in that directory for an idea of how to run each
individual test.
