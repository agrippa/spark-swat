include ${SWAT_HOME}/build.conf

USE_CUDA=0

# DEFINES=-DBRIDGE_DEBUG -DTRACE -DVERBOSE -DPROFILE_OPENCL
DEFINES=
GXX_FLAGS=-O3 -Wall
# GXX_FLAGS+=-check-pointers=rw -check-pointers-dangling=all -g -rdynamic

CLALLOC_HOME=${SWAT_HOME}/clalloc
CLUTIL_HOME=${SWAT_HOME}/clutil

SRC=bridge.cpp common.cpp
INCLUDES=-I${JAVA_HOME}/include -I${JAVA_HOME}/include/darwin \
         -I${JAVA_HOME}/include/linux -I${CLUTIL_HOME} -I${CLALLOC_HOME} \
         -I/System/Library/Frameworks/OpenCL.framework/Versions/Current/Headers \
         -I${CL_HOME}/include
TARGET=libbridge.so

ifeq ($(USE_CUDA),1)
	GXX_FLAGS+=-L/usr/lib/x86_64-linux-gnu -DUSE_CUDA -I${CUDA_HOME}/include
	LIBS=-L${CUDA_HOME}/lib64 -lcuda -lnvrtc
else
	GXX_FLAGS+=-lOpenCL -lrt
	LIBS=-L${CL_HOME}/lib
endif

all: ${TARGET} rerun query_tool

${TARGET}: ${SRC}
	test ! -z "${GXX}" || { echo 'GXX missing, should be defined in SWAT_HOME/build.conf'; exit 1; }
	${GXX} ${DEFINES} ${SRC} -o ${TARGET} ${INCLUDES} ${LIBS} -fPIC --shared \
		-L${CLUTIL_HOME} -L${CLALLOC_HOME} -lclalloc -lclutil ${GXX_FLAGS}

rerun: rerun.cpp common.cpp
	${GXX} ${DEFINES} ${INCLUDES} ${LIBS} rerun.cpp common.cpp -o rerun \
		 -L${CLUTIL_HOME} -lclutil ${GXX_FLAGS}

query_tool: query_tool.cpp
	${GXX} ${INCLUDES} ${LIBS} query_tool.cpp -o query_tool -L${CLUTIL_HOME} -lclutil ${GXX_FLAGS}

clean:
	rm -f ${TARGET} rerun
