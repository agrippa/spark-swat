# Spark SWAT (Spark With Accelerated Tasks)

See the top-level README for information on SWAT.

# Setting up testing for the code generator

The code generator testing infrastructure is stored under the src/test/ folder.
It works by comparing the output of the APARAPI-SWAT code generator with a
reference output stored in a file on disk, and allows you to either view the
difference or update the referenced version if necessary.

Reference kernels for a given platform are stored under:

src/test/scala/org/apache/spark/rdd/cl/tests/<platform-name>

where <platform-name> is derived based on the hostname of the current machine.

If you are setting up a new platform, you should first use the
generate_correct_files.sh script to generate the reference outputs to run future
tests against. You can verify the output by running the test_translator.sh
script. If you a test is broken because the reference output is incorrect (e.g.
due to some new changes to the code generator), you can update that test by
running:

update_test.sh <test-name>

or by simply using update_last_failed_test.sh to fix whichever was the most
recently failed test in a test_translator.sh run.

All scripts above should be run from inside the spark-swat/swat/ directory.
