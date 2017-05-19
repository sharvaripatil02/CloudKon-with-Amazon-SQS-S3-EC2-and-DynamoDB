"""
Client abstraction to Front-End Scheduler
to process tasks on Amazon SQS and DynamoDB
with Local workers and remote workers.

"""

import json
import socket
import sys
import os
from multiprocessing import Process
from collections import defaultdict
import boto3

MY_IP = 'clienthosts' # Used for unique task_ID
MY_PORT = 45001

scheduler_ip = 'localhost' # update manually
scheduler_port = 45001

#workfile = 'task_0_count_10000'

workfile = ''
DEFAULT_BATCH_SIZE = 10

"""
Socket connection to send batches to Front-End Scheduler

"""

def make_connection(batches):
	
	# send as JSON format
	json_obj = json.dumps(batches)
    #creating TCP socket
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Connect to server: 
	sock.connect((scheduler_ip, scheduler_port))
	sock.sendall( json_obj.encode('utf-8') )
	sock.close()
	

"""
processing function to simplyfy tasks and create batches out of it
tasks are sent to Scheduler for execution

"""	
def submit_tasks(data, task_count, type_of_task):
	
	# Constant variables to get Load data
	pending_task = task_count
	curr_batch_size = DEFAULT_BATCH_SIZE
	submitted_task = {}
	count = 0
	
	
	while(pending_task > 0):
		
		# Get current task & pending tasks
		if pending_task > curr_batch_size:
			pending_task = pending_task - curr_batch_size
		else:
			curr_batch_size = pending_task
			pending_task = 0
			
		# stores single task entity: length, task_id, task_type
		batches = {}
		batches["batch_length"] = curr_batch_size
		
		# add terminating condition to client & scheduler
		if pending_task > 0:
			batches["last_batch"] = False
		else:
			batches["last_batch"] = True
		
		# hold list of temp_obj dicts
		task_objects = []
		
		
		for i in range(curr_batch_size):
			# Object will have: task_id, task_type, task_data
			temp_obj = {}
			task_id = str(MY_IP) + '_' + str(MY_PORT) + '_' + str(count) #str(int(time.time())) +
			
			if type_of_task == 'sleep':
				task_type = type_of_task
				task_data = data[count]
				count = count + 1
			
			temp_obj["task_id"] = task_id
			temp_obj["task_type"] = task_type
			temp_obj["task_data"] = task_data
			# All 10 task inserted to task_objects[]
			task_objects.append(temp_obj)
		# All 10 tasks into single batch location
		
		batches["task_objects"] = task_objects
		make_connection(batches)
	# While ends here.....all pending tasks processed


"""
Function to read WorkFile

"""	
def read_file(workfile):
	
	file_exist = 0
	
	if os.path.isfile(workfile):
		# File available in location, read it and build tasks data
		with open(workfile) as fp:
			data = fp.readlines()	
	
		total_tasks = len(data)
		task_type = data[0].split()[0] #identify sleep or cloudkon video
	
	else:
		print('Workfile does not exists.')
		sys.exit(0)
		
	return data, total_tasks, task_type

"""
To get response tasks data from Scheduler after execution of tasks
This checks for submitted tasks and failed tasks
by comparing response received
"""

def get_response():
	
	# Create socket
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	server_address = ('', 45002) # Update manually
	
	received_results = {}
	
	sock.bind(server_address)
	
	# Listen for incoming connections
	sock.listen(5)
	
	last_batch = False
	task_results = []
	
	# Get all batches from Scehduler
	#TODO this will not end till get last-job, if last-job lost
	# we may end up in infinite loop
	
	while(last_batch == False):
	
		con, serv_addr = sock.accept()
		received_json_batch = con.recv(1024)
		received_batch = json.loads(received_json_batch.decode('utf-8'))        
		con.close()
		
		last_batch = received_batch["last_batch"]
		task_results.append( received_batch["task_objects"] )
    	
	print("client got response", len(task_results))
	#print(task_results)
	
	
def main():
	
	# get data from workfile
	data, total_task, task_type = read_file(workfile)
	
	# submit those tasks to scheduler through sockets
	submit_tasks(data, total_task, task_type)
	
	# get status update on received batches
	print("task submitted...")
	
	# get response from server
	get_response()
	


if __name__ == '__main__':
    
    
    if len(sys.argv) == 5 and sys.argv[1] == '-s' and sys.argv[3] == '-w':
        
        #scheduler_ip = sys.argv[2].split(':')[0]
        #scheduler_port = int(sys.argv[2].split(':')[1])
        queuename = sys.argv[2]
        workfile = sys.argv[4]
        
    else:
        print("Usage: <client.py> -s <QueueName> -w <WORKLOAD_FILE>")
        sys.exit(0)
    
    main()
