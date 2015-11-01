import os
import sys
from common import parse_timeline

known_opencl_labels = ['init_write', 'heap', 'run', 'free_index-in',
                       'free_index-out']

if len(sys.argv) != 2:
    print('usage: python breakdown_opencl.py filename')
    sys.exit(1)

per_thread_info = parse_timeline(sys.argv[1])

accumulated_running_time = {}
accumulated_queued_time = {}
accumulated_submitted_time = {}

for thread in per_thread_info:
    for kernel in per_thread_info[thread].get_kernel_list():
      for event in kernel.get_event_list():
          lbl = event.lbl

          if lbl not in accumulated_running_time:
              accumulated_running_time[lbl] = 0
              accumulated_queued_time[lbl] = 0
              accumulated_submitted_time[lbl] = 0

          accumulated_running_time[lbl] = accumulated_running_time[lbl] + \
              event.started_to_finished_ms
          accumulated_queued_time[lbl] = accumulated_queued_time[lbl] + \
              event.queued_to_submitted_ms
          accumulated_submitted_time[lbl] = accumulated_submitted_time[lbl] + \
              event.submitted_to_started_ms

print('Queued:')
for lbl in accumulated_queued_time:
    print(lbl + ': ' + str(accumulated_queued_time[lbl]))
print('')
print('Submitted:')
for lbl in accumulated_submitted_time:
    print(lbl + ': ' + str(accumulated_submitted_time[lbl]))
print('')
print('Running:')
for lbl in accumulated_running_time:
    print(lbl + ': ' + str(accumulated_running_time[lbl]))
