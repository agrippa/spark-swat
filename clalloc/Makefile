include ${SWAT_HOME}/build.conf

# DEFINES=-DBRIDGE_DEBUG -DTRACE -DVERBOSE
TARGET=libclalloc.so
DEFINES=-DVERBOSE

ifeq ($(GXX),nvcc)
GXX_FLAGS=-O3 -Xcompiler '-Wall' -Xcompiler '-fPIC' -DCUDA_ALLOCATOR
else
GXX_FLAGS=-O3 -Wall -fPIC -DOPENCL_ALLOCATOR
endif
INCLUDES=-I${JAVA_HOME}/include -I${JAVA_HOME}/include/darwin \
				 -I${JAVA_HOME}/include/linux

ifneq ($(GXX),nvcc)
IS_CLANG := $(shell ${GXX} --version 2>&1 | grep clang | wc -l | bc)
ifeq ($(IS_CLANG),1)
	GXX_FLAGS += -framework OpenCL
else
	GXX_FLAGS += -lOpenCL
endif
endif

${TARGET}: allocator.cpp allocator.h
	test ! -z "${GXX}" || { echo 'GXX missing, should be defined in SWAT_HOME/build.conf'; exit 1; }
	${GXX} ${GXX_FLAGS} ${DEFINES} allocator.cpp -o libclalloc.so ${INCLUDES} \
		--shared -I${CLUTIL_HOME} -L${CLUTIL_HOME} -lclutil

clean:
	rm -f ${TARGET}
