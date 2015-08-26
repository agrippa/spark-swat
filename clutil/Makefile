include ${SWAT_HOME}/build.conf

# DEFINES=-DBRIDGE_DEBUG -DTRACE -DVERBOSE
TARGET=libclutil.so
DEFINES=
GXX_FLAGS=-O3 -Wall
INCLUDES=-I${JAVA_HOME}/include -I${JAVA_HOME}/include/darwin -I${JAVA_HOME}/include/linux

libclutil.so: ocl_util.cpp ocl_util.h
	test ! -z "${GXX}" || { echo 'GXX missing, should be defined in SWAT_HOME/build.conf'; exit 1; }
	${GXX} ${GXX_FLAGS} ${DEFINES} ocl_util.cpp -o libclutil.so ${INCLUDES} -fPIC --shared

clean:
	rm -f ${TARGET}
