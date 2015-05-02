GXX=g++
DEFINES=-DBRIDGE_DEBUG -DTRACE -DVERBOSE
# DEFINES=
GXX_FLAGS=-g -O0

SRC=bridge.cpp ocl_util.cpp
INCLUDES=-I${JAVA_HOME}/include -I${JAVA_HOME}/include/darwin
TARGET=libbridge.so

IS_CLANG := $(shell ${GXX} --version 2>&1 | grep clang | wc -l | bc)
ifeq ($(IS_CLANG),1)
	GXX_FLAGS += -framework OpenCL
else
	GXX_FLAGS += -lOpenCL
endif

all: ${TARGET} rerun

${TARGET}: ${SRC}
	${GXX} ${GXX_FLAGS} ${DEFINES} ${SRC} -o ${TARGET} ${INCLUDES} -fPIC --shared

rerun: rerun.cpp ocl_util.cpp
	${GXX} ${GXX_FLAGS} ${DEFINES} rerun.cpp ocl_util.cpp -o rerun

clean:
	rm -f ${TARGET} rerun
