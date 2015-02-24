GXX=g++
GXX_FLAGS=-fPIC --shared

SRC=bridge.cpp
INCLUDES=-I${JAVA_HOME}/include -I${JAVA_HOME}/include/darwin
TARGET=libbridge.so

IS_CLANG := $(shell ${GXX} --version | grep clang | wc -l)
ifeq ($(IS_CLANG),1)
	GXX_FLAGS += -framework OpenCL
endif

all: ${SRC}
	g++ ${GXX_FLAGS} ${SRC} -o ${TARGET} ${INCLUDES}
