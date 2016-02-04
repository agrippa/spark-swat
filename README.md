# Spark SWAT (Spark With AcceleraTors)

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
3. **What platforms is SWAT tested on?** As a small project, SWAT is currently
   only tested on HotSpot JDK 1.7.8\_80, Spark 1.5.1, Hadoop 2.4, GCC 4.8.5,
   CUDA 6.5, and NVIDIA GPUs.

# Setup
