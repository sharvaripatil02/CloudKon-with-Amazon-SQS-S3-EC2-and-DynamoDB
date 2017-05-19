import random

# To generate the workload for throughput.

task = "sleep 0"

# Define number of tasks in workload file.
total_task = 100

writefile = "task_0_count_"+str(total_task)
wfile = open(writefile,"ab")

for count in range(total_task):
    wfile.write(task +'\n')
wfile.close()
