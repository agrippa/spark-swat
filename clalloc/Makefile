include ${SWAT_HOME}/build.conf

USE_CUDA=0

# DEFINES=-DBRIDGE_DEBUG -DTRACE -DVERBOSE
TARGET=libclalloc.so
DEFINES=

CLUTIL_HOME=${SWAT_HOME}/clutil

ifeq ($(USE_CUDA),1)
GXX_FLAGS=-O3 -Wall -fPIC -DUSE_CUDA
else
GXX_FLAGS=-O3 -Wall -fPIC
endif

INCLUDES=-I${JAVA_HOME}/include -I${JAVA_HOME}/include/darwin \
         -I${JAVA_HOME}/include/linux -I${CL_HOME}/include

ifeq ($(USE_CUDA),1)
	GXX_FLAGS += -lcudart -I${CUDA_HOME}/include
	LIBS=-L${CUDA_HOME}/lib64
else
	GXX_FLAGS += -lOpenCL
	LIBS=-L${CL_HOME}/lib
endif

${TARGET}: allocator.cpp allocator.h
	test ! -z "${GXX}" || { echo 'GXX missing, should be defined in SWAT_HOME/build.conf'; exit 1; }
	${GXX} ${GXX_FLAGS} ${DEFINES} allocator.cpp -o libclalloc.so ${INCLUDES} ${LIBS} \
		--shared -I${CLUTIL_HOME} -L${CLUTIL_HOME} -lclutil

clean:
	rm -f ${TARGET}
