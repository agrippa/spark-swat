GXX=icpc
# DEFINES=-DBRIDGE_DEBUG -DTRACE -DVERBOSE
DEFINES=
GXX_FLAGS=-O3 -Wall

SRC=bridge.cpp common.cpp
INCLUDES=-I${JAVA_HOME}/include -I${JAVA_HOME}/include/darwin -I${JAVA_HOME}/include/linux -I${CLUTIL_HOME} -I${CLALLOC_HOME}
TARGET=libbridge.so

IS_CLANG := $(shell ${GXX} --version 2>&1 | grep clang | wc -l | bc)
ifeq ($(IS_CLANG),1)
	GXX_FLAGS += -framework OpenCL
else
	GXX_FLAGS += -lOpenCL
endif

all: ${TARGET} rerun query_tool

${TARGET}: ${SRC}
	${GXX} ${GXX_FLAGS} ${DEFINES} ${SRC} -o ${TARGET} ${INCLUDES} -fPIC --shared \
		-L${CLUTIL_HOME} -L${CLALLOC_HOME} -lclalloc -lclutil

rerun: rerun.cpp common.cpp
	${GXX} ${GXX_FLAGS} ${DEFINES} ${INCLUDES} rerun.cpp common.cpp -o rerun -L${CLUTIL_HOME} -lclutil

query_tool: query_tool.cpp
	${GXX} ${GXX_FLAGS} query_tool.cpp -o query_tool -L${CLUTIL_HOME} -lclutil

clean:
	rm -f ${TARGET} rerun
