include ${SWAT_HOME}/build.conf

# USE_CUDA should be set to 1 or 0. If set to 0, we will use OpenCL APIs instead.
USE_CUDA=0

# DEFINES=-DBRIDGE_DEBUG -DTRACE -DVERBOSE
TARGET=libclutil.so
DEFINES=
GXX_FLAGS=-O3 -Wall
INCLUDES=-I${JAVA_HOME}/include -I${JAVA_HOME}/include/darwin \
         -I${JAVA_HOME}/include/linux -I${CL_HOME}/include

ifeq ($(USE_CUDA),1)
	LIBS=-L/usr/lib/x86_64-linux-gnu -L${CUDA_HOME}/lib64
	GXX_FLAGS += -lcuda -lcudart -DUSE_CUDA -I${CUDA_HOME}/include
else
	LIBS=-L${CL_HOME}/lib
	GXX_FLAGS += -lOpenCL
endif


libclutil.so: ocl_util.cpp ocl_util.h
	test ! -z "${GXX}" || { echo 'GXX missing, should be defined in SWAT_HOME/build.conf'; exit 1; }
	${GXX} ${LIBS} ${GXX_FLAGS} ${DEFINES} ocl_util.cpp -o libclutil.so ${INCLUDES} -fPIC --shared

clean:
	rm -f ${TARGET}
