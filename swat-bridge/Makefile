include ${SWAT_HOME}/build.conf

# DEFINES=-DBRIDGE_DEBUG -DTRACE -DVERBOSE
DEFINES=-DVERBOSE
GXX_FLAGS=-O3 -Wall

SRC=bridge.cpp common.cpp
INCLUDES=-I${JAVA_HOME}/include -I${JAVA_HOME}/include/darwin \
         -I${JAVA_HOME}/include/linux -I${CLUTIL_HOME} -I${CLALLOC_HOME} \
         -I/System/Library/Frameworks/OpenCL.framework/Versions/Current/Headers
TARGET=libbridge.so

IS_CLANG := $(shell ${GXX} --version 2>&1 | grep clang | wc -l | bc)
ifeq ($(IS_CLANG),1)
	GXX_FLAGS += -framework OpenCL
else
	GXX_FLAGS += -lOpenCL
endif

all: ${TARGET} rerun query_tool

${TARGET}: ${SRC}
	test ! -z "${GXX}" || { echo 'GXX missing, should be defined in SWAT_HOME/build.conf'; exit 1; }
	${GXX} ${DEFINES} ${SRC} -o ${TARGET} ${INCLUDES} -fPIC --shared \
		-L${CLUTIL_HOME} -L${CLALLOC_HOME} -lclalloc -lclutil ${GXX_FLAGS}

rerun: rerun.cpp common.cpp
	${GXX} ${DEFINES} ${INCLUDES} rerun.cpp common.cpp -o rerun \
		 -L${CLUTIL_HOME} -lclutil ${GXX_FLAGS}

query_tool: query_tool.cpp
	${GXX} query_tool.cpp -o query_tool -L${CLUTIL_HOME} -lclutil ${INCLUDES} ${GXX_FLAGS}

clean:
	rm -f ${TARGET} rerun
