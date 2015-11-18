import sys
import os

known_lock_lbls = ['device_ctxs_lock_contention',
                   'rdd_cache_lock_contention',
                   'kernel_lock_contention',
                   'freed_native_input_buffers_lock_contention',
                   'freed_native_input_buffers_blocked',
                   'completed_kernels_lock_contention',
                   'completed_kernels_blocked',
                   'broadcast_lock_contention',
                   'program_cache_lock_contention',
                   'heap_cache_lock_contention',
                   'heap_cache_blocked',
                   'nloaded_cache_lock_contention',
                   'allocator_contention']

class OpenCLEvent:
    def __init__(self, index, lbl, total_ns, started_ns, queued_to_submitted_ns,
                 submitted_to_started_ns, started_to_finished_ns):
        self.index = index
        self.lbl = lbl
        self.total_ns = total_ns
        self.started_ns = started_ns
        self.queued_to_submitted_ns = queued_to_submitted_ns
        self.submitted_to_started_ns = submitted_to_started_ns
        self.started_to_finished_ns = started_to_finished_ns

    def __str__(self):
        return '[index=' + str(self.index) + ' ' + lbl + ']'


class NormalizedOpenCLEvent:
    def __init__(self, index, lbl, total_ms, started_ms, queued_to_submitted_ms,
                 submitted_to_started_ms, started_to_finished_ms):
        self.index = index
        self.lbl = lbl
        self.total_ms = total_ms
        self.started_ms = started_ms
        self.queued_to_submitted_ms = queued_to_submitted_ms
        self.submitted_to_started_ms = submitted_to_started_ms
        self.started_to_finished_ms = started_to_finished_ms


class LockInfo:
    def __init__(self, thread):
        self.thread = thread
        self.info = {}

    def add_info(self, lbl, t):
        self.info[lbl] = t


class KernelInfo:
    def __init__(self, thread):
        self.thread = thread

        self.input_start = -1
        self.input_elapsed = -1

        self.output_start = -1
        self.output_elapsed = -1

        self.kernel_start_time = -1
        self.opencl_events = {}
        self.normalized_events = {}

    def normalize_opencl_events(self):
        """
        self.kernel_start_time is the ms timestamp at which the OpenCL events
        were initiated. We normalize the OpenCL event timestamps so that the
        started time for the OpenCL stuff is the same as self.kernel_start_time.
        """
        old_events = self.opencl_events
        self.opencl_events = []
        for event_index in old_events.keys():
            event = old_events[event_index]
            total_ms = float(event.total_ns) / 1000000.0
            started_ms = self.kernel_start_time
            queued_to_submitted_ms = float(event.queued_to_submitted_ns) / 1000000.0
            submitted_to_started_ms = float(event.submitted_to_started_ns) / 1000000.0
            started_to_finished_ms = float(event.started_to_finished_ns) / 1000000.0
        
            self.opencl_events.append(NormalizedOpenCLEvent(event.index,
                        event.lbl, total_ms, started_ms, queued_to_submitted_ms,
                        submitted_to_started_ms, started_to_finished_ms))

    def get_event_list(self):
        return self.opencl_events

    def str(self):
        return '{ thread=' + str(self.thread) + ', input_start=' + \
               str(self.input_start) + ', input_elapsed=' + \
               str(self.input_elapsed) + ', output_start=' + \
               str(self.output_start) + ', output_elapsed=' + \
               str(self.output_elapsed) + ', kernel_start_time=' + \
               str(self.kernel_start_time) + ' }'

    def has_input_info(self):
        return self.input_start != -1

    def has_output_start(self):
        return self.output_start != -1

    def has_output_elapsed(self):
        return self.output_elapsed != -1

    def has_kernel_start_info(self):
        return self.kernel_start_time != -1

    def has_opencl_event(self, event_index):
        return event_index in self.opencl_events

    def get_input_start(self):
        return self.input_start

    def get_input_elapsed(self):
        return self.input_elapsed

    def get_output_start(self):
        return self.output_start

    def get_output_elapsed(self):
        return self.output_elapsed

    def set_input_time(self, start, elapsed):
        assert self.input_start == -1 and self.input_elapsed == -1
        self.input_start = start
        self.input_elapsed = elapsed

    def set_output_start(self, start):
        assert self.output_start == -1
        self.output_start = start

    def set_output_elapsed(self, end):
        assert self.output_start != -1 and self.output_elapsed == -1
        self.output_elapsed = end - self.output_start

    def set_kernel_start_time(self, s):
        assert self.kernel_start_time == -1
        self.kernel_start_time = s

    def add_opencl_event(self, thread, index, lbl, total_ns, started,
                         queued_to_submitted, submitted_to_started,
                         started_to_finished):
        assert self.thread == thread
        event = OpenCLEvent(index, lbl, total_ns, started, queued_to_submitted,
                            submitted_to_started, started_to_finished)
        assert index not in self.opencl_events.keys()
        self.opencl_events[index] = event

    def get_nevents(self):
        return len(self.opencl_events)

    def complete(self):
        nevents = len(self.opencl_events)
        for i in range(nevents):
            if i not in self.opencl_events.keys():
                return False
        return self.thread >= 0 and \
               self.input_start >= 0 and self.input_elapsed >= 0 and \
               self.output_start >= 0 and self.output_elapsed >= 0 and \
               self.kernel_start_time >= 0


class ThreadKernelInfo:
    def __init__(self, thread):
        self.thread = thread
        self.kernels = [] # information on each kernel instance
        self.lock_info = LockInfo(thread)
        self.total_time = 0

    def add_to_total_time(self, t):
        self.total_time = self.total_time + t

    def get_kernel_list(self):
        return self.kernels

    def check_all_complete(self):
        for k in self.kernels:
            assert k.complete(), k.str()

    def create_new_kernel(self):
        kernel = KernelInfo(self.thread)
        self.kernels.append(kernel)
        return kernel

    def add_input_info(self, start, elapsed):
        i = 0
        while i < len(self.kernels) and self.kernels[i].has_input_info():
            i += 1
        assert i < len(self.kernels)
        self.kernels[i].set_input_time(start, elapsed)

    def add_output_start(self, start):
        i = 0
        while i < len(self.kernels) and self.kernels[i].has_output_start():
            i += 1
        assert i < len(self.kernels)
        self.kernels[i].set_output_start(start)

    def add_output_end(self, end):
        i = 0
        while i < len(self.kernels) and self.kernels[i].has_output_elapsed():
            i += 1
        assert i < len(self.kernels)
        self.kernels[i].set_output_elapsed(end)

    def add_kernel_start(self, start):
        i = 0
        while i < len(self.kernels) and self.kernels[i].has_kernel_start_info():
            i += 1
        assert i < len(self.kernels)
        self.kernels[i].set_kernel_start_time(start)

    def add_opencl_event(self, index, lbl, total_ns, started,
                         queued_to_submitted, submitted_to_started,
                         started_to_finished):
        i = 0
        while i < len(self.kernels) and self.kernels[i].has_opencl_event(index):
            i += 1
        assert i < len(self.kernels)
        self.kernels[i].add_opencl_event(self.thread, index, lbl, total_ns, started,
                         queued_to_submitted, submitted_to_started,
                         started_to_finished)

    def get_lock_info(self):
        return self.lock_info


def get_thread_info(thread, per_thread_info):
    if thread not in per_thread_info.keys():
        per_thread_info[thread] = ThreadKernelInfo(thread)
    return per_thread_info[thread]


def parse_timeline(filename):
    per_thread_info = {}
    fp = open(filename, 'r')

    for line in fp:
        if line.startswith('SWAT PROF') and not line.startswith('SWAT PROF Total loaded'):
            tokens = line.split()
            event = tokens[3]
            thread = int(tokens[2])
            if event == 'Input-I/O':
                input_elapsed = int(tokens[4])
                input_start = int(tokens[6]) - input_elapsed
    
                thread_info = get_thread_info(thread, per_thread_info)
                kernel_info = thread_info.create_new_kernel()
                thread_info.add_input_info(input_start, input_elapsed)
            elif event == 'Total':
                t = int(tokens[4])
                thread_info = get_thread_info(thread, per_thread_info)
                thread_info.add_to_total_time(t)
            elif event == 'Kernel' and tokens[4] == 'launch':
                seq = int(tokens[10])
                thread_info = get_thread_info(thread, per_thread_info)
                start_time = int(tokens[6])
                thread_info.add_kernel_start(start_time)
            elif event == 'Started' and tokens[4] == 'writing':
                t = int(tokens[9])
                thread_info = get_thread_info(thread, per_thread_info)
                thread_info.add_output_start(t)
            elif event == 'Finished' and tokens[4] == 'writing':
                t = int(tokens[9])
                thread_info = get_thread_info(thread, per_thread_info)
                thread_info.add_output_end(t)
        elif 'queued -> submitted' in line:
            # thread 10 : seq 0 : 0 : init_write : 135456 ns total (started = 12173461684, queued -> submitted 27616 ns, submitted -> started 10080 ns, started -> finished 97760 ns)
            tokens = line.split()
            thread = int(tokens[1])
            seq = int(tokens[4])
            index = int(tokens[6])
            lbl = tokens[8]
    
            total_ns = int(tokens[10])
    
            started_str = tokens[15];
            started_str = started_str[0:len(started_str) - 1]
            started = int(started_str)
    
            queued_to_submitted_str = tokens[19]
            queued_to_submitted = int(queued_to_submitted_str)
            
            submitted_to_started_str = tokens[24]
            submitted_to_started = int(submitted_to_started_str)
    
            started_to_finished_str = tokens[29]
            started_to_finished = int(tokens[29])
    
            thread_info = get_thread_info(thread, per_thread_info)
            thread_info.add_opencl_event(index, lbl, total_ns, started,
                                         queued_to_submitted, submitted_to_started,
                                         started_to_finished)
        elif 'LOCK : ' in line:
            tokens = line.split()
            lbl = tokens[4]
            thread = int(tokens[2])
    
            lock_info = get_thread_info(thread, per_thread_info).get_lock_info()
            t = int(tokens[len(tokens) - 1])
            assert lbl in known_lock_lbls
            lock_info.add_info(lbl, t)

    for thread in per_thread_info.keys():
        thread_info = per_thread_info[thread]
        thread_info.check_all_complete()
        for kernel in thread_info.get_kernel_list():
            kernel.normalize_opencl_events()

    fp.close()
    return per_thread_info
