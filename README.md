Spark SWAT (Spark With AcceleraTors)

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
   [https://github.com/agrippa/aparapi-swat].

# FAQs

# Setup
