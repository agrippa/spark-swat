#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <map>

#include "ocl_util.h"
#include "common.h"
#include "kernel_arg.h"

using namespace std;

enum ARG_TYPE {
    INT,
    FLOAT,
    DOUBLE,
    LONG
};

typedef struct _output_arg {
    int index;
    ARG_TYPE type;
} output_arg;

void usage(char **argv) {
    fprintf(stderr, "usage: %s -i file -d device -h -l -p -k kernel-file "
            "-o index:type -v\n", argv[0]);
}

void get_arg_type_index(char *arg, ARG_TYPE *out_type, int *out_index) {
    char *found = strchr(arg, ':');
    assert(found != NULL);

    *found = '\0';
    *out_index = atoi(arg);

    char *type_str = found + 1;
    if (strcmp(type_str, "float") == 0) {
        *out_type = FLOAT;
    } else if (strcmp(type_str, "int") == 0) {
        *out_type = INT;
    } else if (strcmp(type_str, "double") == 0) {
        *out_type = DOUBLE;
    } else if (strcmp(type_str, "long") == 0) {
        *out_type = LONG;
    } else {
        fprintf(stderr, "Unsupported type \"%s\"\n", type_str);
        exit(1);
    }
}

void list_devices() {
    cl_uint num_platforms = get_num_opencl_platforms();
    cl_platform_id *platforms =
        (cl_platform_id *)malloc(sizeof(cl_platform_id) * num_platforms);
    CHECK(clGetPlatformIDs(num_platforms, platforms, NULL));
    int device_index = 0;

    for (cl_uint platform_index = 0; platform_index < num_platforms;
            platform_index++) {
        cl_uint num_devices = get_num_devices(platforms[platform_index]);

        printf("Platform %d (%d devices)\n", platform_index, num_devices);

        cl_device_id *devices = (cl_device_id *)malloc(sizeof(cl_device_id) *
                num_devices);
        CHECK(clGetDeviceIDs(platforms[platform_index], CL_DEVICE_TYPE_ALL,
                    num_devices, devices, NULL));
        for (unsigned d = 0; d < num_devices; d++) {
            cl_device_id dev = devices[d];
            cl_device_type type;
            CHECK(clGetDeviceInfo(dev, CL_DEVICE_TYPE, sizeof(type), &type,
                        NULL));
            printf("  Device %d - ", device_index);

            if (type == CL_DEVICE_TYPE_GPU) {
                printf("GPU");
            } else if (type == CL_DEVICE_TYPE_CPU) {
                printf("CPU");
            } else {
                fprintf(stderr, "Unsupported device type in list_devices\n");
                exit(1);
            }

            printf(" - ");

            size_t name_len;
            CHECK(clGetDeviceInfo(dev, CL_DEVICE_NAME, 0, NULL, &name_len));
            char *device_name = (char *)malloc(name_len + 1);
            CHECK(clGetDeviceInfo(dev, CL_DEVICE_NAME, name_len, device_name,
                        NULL));
            device_name[name_len] = '\0';

            size_t version_len;
            CHECK(clGetDeviceInfo(dev, CL_DEVICE_VERSION, 0, NULL, &version_len));
            char *device_version = (char *)malloc(version_len + 1);
            CHECK(clGetDeviceInfo(dev, CL_DEVICE_VERSION, version_len, device_version, NULL));
            device_version[version_len] = '\0';

            cl_uint compute_units;
            CHECK(clGetDeviceInfo(dev, CL_DEVICE_MAX_COMPUTE_UNITS, sizeof(compute_units), &compute_units, NULL));

            size_t ext_len;
            CHECK(clGetDeviceInfo(dev, CL_DEVICE_EXTENSIONS, 0, NULL, &ext_len));
            char *device_ext = (char *)malloc(ext_len + 1);
            CHECK(clGetDeviceInfo(dev, CL_DEVICE_EXTENSIONS, ext_len, device_ext, NULL));
            device_ext[ext_len] = '\0';

            printf("%s - %d compute units - %s - %s\n", device_name, compute_units, device_version, device_ext);

            device_index++;
        }
        free(devices);
    }
    free(platforms);
}

void find_platform_and_device(unsigned target_device, cl_platform_id *platform,
        cl_device_id *device) {
    cl_uint num_platforms = get_num_opencl_platforms();
    cl_platform_id *platforms =
        (cl_platform_id *)malloc(sizeof(cl_platform_id) * num_platforms);
    CHECK(clGetPlatformIDs(num_platforms, platforms, NULL));
    unsigned device_index = 0;

    for (cl_uint platform_index = 0; platform_index < num_platforms;
            platform_index++) {
        cl_uint num_devices = get_num_devices(platforms[platform_index]);

        cl_device_id *devices = (cl_device_id *)malloc(sizeof(cl_device_id) *
                num_devices);
        CHECK(clGetDeviceIDs(platforms[platform_index], CL_DEVICE_TYPE_ALL,
                    num_devices, devices, NULL));
        if (target_device >= device_index && target_device < device_index + num_devices) {
            *device = devices[target_device - device_index];
            *platform = platforms[platform_index];

            free(devices);
            free(platforms);
            return;
        }

        free(devices);
        device_index += num_devices;
    }
    free(platforms);

    fprintf(stderr, "Failed to find target device %d\n", target_device);
    exit(1);
}

bool want_to_clear(int index, int *clears, int nclears) {
    for (int i = 0; i < nclears; i++) {
        if (clears[i] == index) return true;
    }
    return false;
}

int main(int argc, char **argv) {
#ifndef BRIDGE_DEBUG
    fprintf(stderr, "Error, %s was not compiled with -DBRIDGE_DEBUG\n",
            argv[0]);
    return (1);

#else

    char *input_file = NULL;
    char *kernel_file = NULL;
    output_arg *output_args = NULL;
    int n_output_args = 0;
    int device = -1;
    bool print_kernel = false;
    bool verbose = false;

    int *clears = NULL;
    int nclears = 0;

    int c;
    opterr = 0;
    while ((c = getopt(argc, argv, "i:d:hlpk:o:vc:")) != -1) {
        switch (c) {
            case 'c':
                clears = (int *)realloc(clears, (nclears + 1) * sizeof(int));
                clears[nclears] = atoi(optarg);
                nclears++;
                break;
            case 'v':
                verbose = true;
                break;
            case 'o':
                output_args = (output_arg *)realloc(output_args,
                        sizeof(output_arg) * (n_output_args + 1));
                get_arg_type_index(optarg, &output_args[n_output_args].type,
                        &output_args[n_output_args].index);
                n_output_args++;
                break;
            case 'k':
                kernel_file = optarg;
                break;
            case 'p':
                print_kernel = true;
                break;
            case 'i':
                input_file = optarg;
                break;
            case 'd':
                device = atoi(optarg);
                break;
            case 'h':
                usage(argv);
                return (1);
            case 'l':
                list_devices();
                return (0);
            case '?':
                fprintf(stderr, "Invalid option %c\n", optopt);
                usage(argv);
                return (1);
            default:
                fprintf(stderr, "Should not have gotten here...\n");
                return (1);
        }
    }

    if (input_file == NULL) {
        printf("An input file must be specified with CLI flag -i\n");
        usage(argv);
        return (1);
    }

    if (device == -1) {
        printf("A device to use must be specified with CLI flag -d\n");
        usage(argv);
        return (1);
    }

    // Load kernel and arguments from dump file
    int fd = open(input_file, O_RDONLY, 0);
    if (fd == -1) {
        fprintf(stderr, "Error reading from file %s\n", input_file);
        return (1);
    }

    size_t kernel_src_len;
    char *kernel_src;
    safe_read(fd, &kernel_src_len, sizeof(kernel_src_len));
    kernel_src = (char *)malloc(kernel_src_len + 1);
    safe_read(fd, kernel_src, kernel_src_len + 1);

    if (kernel_file != NULL) {
        FILE *fp = fopen(kernel_file, "r");
        assert(fp != NULL);
        fseek(fp, 0, SEEK_END);
        size_t fsize = ftell(fp);
        fseek(fp, 0, SEEK_SET);

        kernel_src = (char *)realloc(kernel_src, fsize + 1);
        kernel_src_len = fsize;
        size_t nread = fread(kernel_src, 1, fsize, fp);
        if (nread != fsize) {
            fprintf(stderr, "Expected to read %lu but got %lu\n", fsize, nread);
            perror("kernel read");
            exit(1);
        }

        fclose(fp);
    }

    if (print_kernel) {
        printf("%s\n", kernel_src);
    }

    int num_args, i;
    safe_read(fd, &num_args, sizeof(num_args));

    map<int, kernel_arg *> debug_arguments;
    map<int, cl_mem> arguments;
    for (i = 0; i < num_args; i++) {
        int arg_index;
        safe_read(fd, &arg_index, sizeof(arg_index));
        kernel_arg *arg = new kernel_arg(fd);

        fprintf(stderr, "Read arg %d of size %lu, val=%p, is_ref? %s, zero? "
                "%s\n", arg_index, arg->get_size(), arg->get_val(),
                arg->get_is_ref() ? "true" : "false",
                arg->get_clear_to_zero() ? "true" : "false");

        debug_arguments[arg_index] = arg;
    }

    for (map<int, kernel_arg *>::iterator i = debug_arguments.begin(),
            e = debug_arguments.end(); i != e; i++) {
        int arg_index = i->first;
        kernel_arg *arg = i->second;
    }

    close(fd);

    // Set up OpenCL environment
    cl_platform_id cl_platform;
    cl_device_id cl_device;
    find_platform_and_device(device, &cl_platform, &cl_device);

    size_t name_len;
    CHECK(clGetDeviceInfo(cl_device, CL_DEVICE_NAME, 0, NULL, &name_len));
    char *device_name = (char *)malloc(name_len + 1);
    CHECK(clGetDeviceInfo(cl_device, CL_DEVICE_NAME, name_len, device_name,
                NULL));
    device_name[name_len] = '\0';
    fprintf(stderr, "Using device %s\n", device_name);

    cl_int err;
    cl_context_properties ctx_props[] = { CL_CONTEXT_PLATFORM,
        (cl_context_properties)cl_platform, 0 };
    cl_context ctx = clCreateContext(ctx_props, 1, &cl_device, NULL, NULL,
            &err);
    CHECK(err);

    cl_command_queue cmd = clCreateCommandQueue(ctx, cl_device, 0, &err);
    CHECK(err);

    size_t source_size[] = { kernel_src_len };
    cl_program program = clCreateProgramWithSource(ctx, 1,
            (const char **)&kernel_src, source_size, &err);
    CHECK(err);

    err = clBuildProgram(program, 1, &cl_device, NULL, NULL, NULL);
    if (verbose || err == CL_BUILD_PROGRAM_FAILURE) {
        size_t build_log_size;
        CHECK(clGetProgramBuildInfo(program, cl_device, CL_PROGRAM_BUILD_LOG, 0,
                    NULL, &build_log_size));
        char *build_log = (char *)malloc(build_log_size + 1);
        CHECK(clGetProgramBuildInfo(program, cl_device, CL_PROGRAM_BUILD_LOG,
                    build_log_size, build_log, NULL));
        build_log[build_log_size] = '\0';
        fprintf(stderr, "Build log:\n%s\n", build_log);
        free(build_log);
    }
    CHECK(err);

    cl_kernel kernel = clCreateKernel(program, "run", &err);
    CHECK(err);

    for (map<int, kernel_arg *>::iterator i = debug_arguments.begin(),
            e = debug_arguments.end(); i != e; i++) {
        int arg_index = i->first;
        kernel_arg *arg = i->second;
        if (arg->get_is_ref()) {
            cl_int err;
            cl_mem mem = clCreateBuffer(ctx, CL_MEM_READ_WRITE,
                    arg->get_size(), NULL, &err);
            CHECK(err);
            fprintf(stderr, "Allocating argument %d of size %lu\n", arg_index,
                    arg->get_size());

            assert(arguments.find(arg_index) == arguments.end());
            arguments[arg_index] = mem;

            if (arg->get_val() == NULL) {
                if (arg->get_clear_to_zero() || want_to_clear(arg_index, clears, nclears)) {
                    fprintf(stderr, "  Memsetting to zeros\n");
                    void *zeros = malloc(arg->get_size());
                    memset(zeros, 0x00, arg->get_size());
                    CHECK(clEnqueueWriteBuffer(cmd, mem, CL_TRUE, 0, arg->get_size(), zeros, 0, NULL, NULL));
                }
            } else {
                assert(!arg->get_clear_to_zero());
                fprintf(stderr, "  Filling...\n");
                CHECK(clEnqueueWriteBuffer(cmd, mem, CL_TRUE, 0,
                            arg->get_size(), arg->get_val(), 0, NULL, NULL));
            }

            CHECK(clSetKernelArg(kernel, arg_index, sizeof(mem), &mem));
        } else {
            assert(!arg->get_clear_to_zero());
            assert(arg->get_val() != NULL);
            fprintf(stderr, "Scalar argument for %d\n", arg_index);
            CHECK(clSetKernelArg(kernel, arg_index, arg->get_size(),
                        arg->get_val()));
        }
    }
    CHECK(clFinish(cmd));

    cl_event event;
    size_t range = 1024;
    CHECK(clEnqueueNDRangeKernel(cmd, kernel, 1, NULL, &range, NULL, 0, NULL,
                &event));
    CHECK(clWaitForEvents(1, &event));
    CHECK(clFinish(cmd));

    for (int i = 0; i < n_output_args; i++) {
        output_arg arg = output_args[i];
        assert(debug_arguments.find(arg.index) != debug_arguments.end());
        kernel_arg *karg = debug_arguments[arg.index];
        size_t size = karg->get_size();

        fprintf(stderr, "Outputting argument #%d\n", arg.index);

        if (karg->get_is_ref()) {
            assert(arguments.find(arg.index) != arguments.end());

            cl_mem mem = arguments[arg.index];
            switch (arg.type) {
                case (INT): {
                    assert(size % sizeof(int) == 0);
                    int *ibuf = (int *)malloc(size);
                    CHECK(clEnqueueReadBuffer(cmd, mem, CL_TRUE, 0, size, ibuf, 0,
                                NULL, NULL));
                    for (int j = 0; j < (size / sizeof(int)); j++) {
                        fprintf(stderr, "  %d\n", ibuf[j]);
                    }
                    fprintf(stderr, "\n");
                    free(ibuf);
                    break;
                }

                case (FLOAT): {
                    assert(size % sizeof(float) == 0);
                    float *fbuf = (float *)malloc(size);
                    CHECK(clEnqueueReadBuffer(cmd, mem, CL_TRUE, 0, size, fbuf, 0,
                                NULL, NULL));
                    for (int j = 0; j < (size / sizeof(float)); j++) {
                        fprintf(stderr, "%f\n", fbuf[j]);
                    }
                    fprintf(stderr, "\n");
                    free(fbuf);
                    break;
                }

                case (DOUBLE): {
                    assert(size % sizeof(double) == 0);
                    double *dbuf = (double *)malloc(size);
                    CHECK(clEnqueueReadBuffer(cmd, mem, CL_TRUE, 0, size, dbuf, 0,
                                NULL, NULL));
                    for (int j = 0; j < (size / sizeof(double)); j++) {
                        fprintf(stderr, "%f\n", dbuf[j]);
                    }
                    fprintf(stderr, "\n");
                    free(dbuf);
                    break;
                }

                case (LONG): {
                    assert(size % sizeof(long) == 0);
                    long *lbuf = (long *)malloc(size);
                    CHECK(clEnqueueReadBuffer(cmd, mem, CL_TRUE, 0, size, lbuf,
                                0, NULL, NULL));
                    for (int j = 0; j < (size / sizeof(long)); j++) {
                        fprintf(stderr, "%ld ", lbuf[j]);
                    }
                    fprintf(stderr, "\n");
                    free(lbuf);
                    break;
                }

                default:
                    fprintf(stderr, "Unknown type\n");
                    exit(1);
            }
        } else {
            assert(karg->get_val() != NULL);
            switch(arg.type) {
                case (INT):
                    assert(size == sizeof(int));
                    fprintf(stderr, "%d\n", *((int *)karg->get_val()));
                    break;
                case (FLOAT):
                    assert(size == sizeof(float));
                    fprintf(stderr, "%f\n", *((float *)karg->get_val()));
                    break;
                case (DOUBLE):
                    assert(size == sizeof(double));
                    fprintf(stderr, "%f\n", *((double *)karg->get_val()));
                    break;
                case (LONG):
                    assert(size == sizeof(long));
                    fprintf(stderr, "%ld\n", *((long *)karg->get_val()));
                    break;
                default:
                    fprintf(stderr, "Unknown type\n");
                    exit(1);
            }
        }
    }

    return (0);
#endif
}
