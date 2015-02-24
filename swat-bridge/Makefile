GXX=g++
GXX_FLAGS=-fPIC --shared

SRC=bridge.cpp
INCLUDES=-I${JAVA_HOME}/include -I${JAVA_HOME}/include/darwin
TARGET=libbridge.so

IS_CLANG := $(shell ${GXX} --version 2>&1 | grep clang | wc -l | bc)
ifeq ($(IS_CLANG),1)
	GXX_FLAGS += -framework OpenCL
else
	GXX_FLAGS += -lOpenCL
endif

all: ${SRC}
	${GXX} ${GXX_FLAGS} ${SRC} -o ${TARGET} ${INCLUDES}
