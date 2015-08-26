GXX=icpc
# DEFINES=-DBRIDGE_DEBUG -DTRACE -DVERBOSE
DEFINES=
GXX_FLAGS=-O3 -Wall
INCLUDES=-I${JAVA_HOME}/include -I${JAVA_HOME}/include/darwin -I${JAVA_HOME}/include/linux

libclutil.so: ocl_util.cpp ocl_util.h
	${GXX} ${GXX_FLAGS} ${DEFINES} ocl_util.cpp -o libclutil.so ${INCLUDES} -fPIC --shared
