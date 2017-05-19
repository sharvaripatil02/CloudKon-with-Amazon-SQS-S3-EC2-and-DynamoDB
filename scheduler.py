"""
Scheduler to perform scheduling of local and back end workers
this acts as primary route to local and back-end remote workers
performs fetching JSON data, executing tasks, and sending response back to client
It uses Python BOTO3 library

Sunny Changediya
"""


import json
import socket
import sys
import os
import time
from multiprocessing import Process, Queue
from collections import defaultdict
import boto3

# Update manually these parameters
client_ip = 'localhost'
client_port = 45002
scheduler_ip = 'localhost'
scheduler_port = 45001

no_of_threads = 4
worker_type = ''

DEFAULT_BATCH_SIZE = 10


"""
Working On Local Client & Worker Part

"""
def extract_data():

	# create socket, bind it, listen & accept
	server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	
	# bind it
	sched_address = (scheduler_ip, scheduler_port)
	server_sock.bind(sched_address)
	
	# listen on it
	server_sock.listen(1)
		
	total_task_added = 0
	temp_dict = {}
	
	# iniitalize in-memory Queue
	queue = Queue()
	queue_size = 0
	
	last_job = False
	while(last_job != True):
		
		# accept from socket, decode json object, store task into Queue
		conn, client_addr = server_sock.accept()
		#print("server connection accepted")
		
		# receive JSON batch
		got_json_batch = conn.recv(1024)
		#print("json batch received")		
		
		# Encode JSON batch
		got_batch = json.loads( got_json_batch.decode('utf-8') )
		
		#print("loaded json batch")
		conn.close()

		# get list of dicts, extract fields
		msg_list = got_batch["task_objects"]
		msg_length = got_batch["batch_length"]
		# terminating condition
		last_job = got_batch["last_batch"]
				
		#print("extarcted things")
				
		cnt = 0
		while(cnt < msg_length):
		# extract from task_objects list
			server_batch = {}
			server_batch["id"] = msg_list[cnt]["task_id"]
			server_batch["type"] = msg_list[cnt]["task_type"]
			server_batch["data"] = msg_list[cnt]["task_data"]
			
			# Check for Remote & Local workers TAsk
			if worker_type == 'lw':		
				# add it into queue
				queue.put(server_batch)
				queue_size = queue_size + 1
				#print("data put into queue")
				
			elif worker_type == 'rw':
				# add it into dict
				temp_dict[total_task_added] = server_batch
				#print("data put into DICT")

			cnt = cnt + 1
			total_task_added = total_task_added + 1
		# this while used to add to Queue
	
	if worker_type == 'lw':
		#print("All data added to local queue")
		return queue, queue.qsize() #total_task_added
	elif worker_type == 'rw':
		return temp_dict
	

"""
Function to accept tasks from Client at first

"""
def get_client_data():

		
	# Start accepting it now ...
	done = False
	while(done != True):
		
		
		if worker_type == 'lw': # Local Workers
			
			mem_queue = Queue()
			mem_queue, total_tasks = extract_data()
			done = True #when next_batch=0 reached
		
		elif worker_type == 'rw': # Remote workers...

			temp_dict = extract_data()
			done = True	
	
	# while(done) ends, means connection closes
	
	if worker_type == 'lw':
		return mem_queue, total_tasks
	elif worker_type == 'rw':
		return temp_dict
	

"""
Multiprocessign function to perform threading operation
and actual sleep task execution in local worker mode only

"""
def worker(queue, res_queue):	
	
	
	# actual sleep execution, get sleep value
	if queue.empty():
		#print("item nahiye re baba")
		sys.exit()
		#temp_obj = {}
		#res_queue.put(temp_obj)
	
	else:
		# Get Queue item
		q_val = queue.get()
		sleep_value = int(q_val["data"].split()[1])
		
		time.sleep(sleep_value) #time of sleep in seconds
		
		temp_obj = {}
		
		# Extract the fields of JSON batch
		temp_obj["id"] = q_val["id"]
		temp_obj["type"] = q_val["type"]
		temp_obj["data"] = q_val["data"]
		
		# Update to ResponseQueue
		res_queue.put(temp_obj)
	

"""
Function to perform Local worker tasks and in-memory
queue handling

"""
def process_local_tasks(queue,total_task):
	
	# response Queue
	res_queue = Queue()
	
	#print("Into Process Local Data and total task are %d" %queue.qsize())
	
	i = 0
	while i < total_task:
	
		procs = []
		#print("Local task processed %d & total Task %d" %(i, total_task))
		
		for k in range(no_of_threads):
			
			# call multiprocess...Create Child processes...
			if (i+k) >= total_task:
				print("ohh breaking now")
				break
			else:
				# creates Multi-Processing Threads...(i+k): queue index
				child_proc = Process( target=worker, args=(queue, res_queue) )
				procs.append(child_proc)
				child_proc.start()
		
		# Join and wait for processes to finish...
		for n in procs:
			n.join()
			
		# increment count by Child process count
		i = i + no_of_threads
	
	# While ends here...
	print('returning Local Response Queue')
	return res_queue

"""

Working on Remote Worker & Client Data

"""


def process_remote_tasks(temp_dict):

	queue_name = 'RequestQueue'	
			
	# Get the 'sqs' service resource
	sqs = boto3.resource('sqs')
	# Create SQS Queue
	my_queue = sqs.create_queue(QueueName=queue_name, Attributes={'DelaySeconds': '0'})
	# Get Queue URL
	queueurl = my_queue.url
	print("Request Queue URL is %s" %(queueurl) )
    
	# Send json_obj directly to sqs.send_message()
	# DO not include "encode", otherwise convert it to str(json.encode) 
	# and send. It will add extra bytes to sqs
    
	for i in range(len(temp_dict)):
   	
		json_obj = json.dumps(temp_dict[i])
		#json_batch = json_obj.encode('utf-8')
  	
		request = my_queue.send_message(MessageBody=json_obj)
	
	"""
	Create Response Queue & DynamoDB Tables for remote workers
	"""
	
	# create Response Queue
	resqueue = sqs.create_queue(QueueName='ResponseQueue', Attributes={'DelaySeconds': '0'})
	

"""
make socket connection and send data to
RequestQueue over network

"""
def make_connection(batches):
	
	#print("server response to client")
	json_obj = json.dumps(batches)
	
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	# Connect to server: 
	sock.connect((client_ip, 45002))
	sock.sendall(json_obj.encode('utf-8') )
	sock.close()   		
	#print("all server response sent")

"""
utility function to send response back to Client

"""
def send_response(res_queue):
	
	q_size = res_queue.qsize()
	pending_task = q_size
	
	curr_batch_size = DEFAULT_BATCH_SIZE
	count = 0
	
	batches = {}
	task_objects = []
	
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
			temp_obj = res_queue.get()
			task_objects.append(temp_obj)
			
		# All 10 tasks into single batch location
		
		batches["task_objects"] = task_objects
		make_connection(batches)
	# While ends here.....all pending tasks processed


"""
Send responsce from Remote workers task execution
to client
"""
def send_remote_response(resp_list):
	
	size = len(resp_list)
	
	pending_task = size
	
	curr_batch_size = DEFAULT_BATCH_SIZE
	count = 0
	
	new_batches = {}
	task_objects = []
	
	while(pending_task > 0):
		
		# Get current task & pending tasks
		if pending_task > curr_batch_size:
			pending_task = pending_task - curr_batch_size
		else:
			curr_batch_size = pending_task
			pending_task = 0
		# stores single task entity: length, task_id, task_type
		new_batches = {}
		new_batches["batch_length"] = curr_batch_size
		# add terminating condition to client & scheduler
		if pending_task > 0:
			new_batches["last_batch"] = False
		else:
			new_batches["last_batch"] = True
		# hold list of temp_obj dicts
		task_objects = []
		
		for i in range(curr_batch_size):
			# Object will have: task_id, task_type, task_data
			temp_obj = resp_list[i]
			task_objects.append(temp_obj)
			
		# All 10 tasks into single batch location
		new_batches["task_objects"] = task_objects
		make_connection(new_batches)
	# While ends here.....all pending tasks processed


"""
Receive response from Remote worker execution
after remote worker finish execution, they send response
back to scheduler which inturn sends response to Client

"""
def get_remote_response(response_queue, total_msg):
	
	res_queue = 'ResponseQueue'
	
	unique_tasks = []
	duplicates = []
	
	total_msg_received = 0
	
	# Get the service resource
	sqs = boto3.resource('sqs')
	
	# Get the queue instance
	queue = sqs.get_queue_by_name(QueueName='ResponseQueue')
	
	que = sqs.Queue(queue.url)
	i = 0
	batch_list = []
	
	while (i < total_msg):
		
		# it returns a list of single message, so msg is a list
		msg = queue.receive_messages()
		
		if msg:
			
			i = i + 1
			batch = json.loads(msg[0].body)
			
			# Delete message from response Queue First
			msg[0].delete()
			print("RMW: Message deleted from Response Queue")
			batch_list.append(batch)
	
	###
	print("All Messages got from Response Queue")
	return batch_list


"""
Main handler Function

"""

def main():
	
	memory_queue = Queue()
	response_queue = Queue()
	
	# get data from client creating socket & put into Queue
	
	if worker_type == 'lw': # local worker
		
		print("Into LW Mode")
		memory_queue, que_task = get_client_data()
		# multiprocessing & executing Tasks
		
		start_time = time.time()
		response_queue = process_local_tasks(memory_queue, que_task)
		end_time = time.time()
		
		print("\nTime Taken by Local Workers is %s sec" %(end_time - start_time) )
		
		#print("success on server dada")
		send_response(response_queue)
		print("server response sent successfully")

	elif worker_type == 'rw': # Remote Worker
		
		print("Into RW Mode.")
		temp_dict = get_client_data()
		
		# Early Create DynamoDb table
		client = boto3.client('dynamodb')
		
		# DynamoDb table creation for Remote worker task execution handling
		
		table = client.create_table(TableName='TaskUsers',KeySchema=[{'AttributeName': 'file_id','KeyType': 'HASH'}],AttributeDefinitions=[{'AttributeName': 'file_id','AttributeType': 'S'}],ProvisionedThroughput={'ReadCapacityUnits': 5,'WriteCapacityUnits': 5})
		
		# Update DynamoDB table with already processed or 
		# currently being processed tasks
		queue_size = len(temp_dict)
		process_remote_tasks(temp_dict)
		print("RW: messages sent")
		resp_list = get_remote_response(response_queue, queue_size)
		send_remote_response(resp_list)
	
	

if __name__ == '__main__':
    
    
    if len(sys.argv) == 5 and sys.argv[1] == '-s' and sys.argv[3] == '-t':
        
        #scheduler_ip = sys.argv[2].split(':')[0]
        #scheduler_port = int(sys.argv[2].split(':')[1])
        qname = sys.argv[2]
        no_of_thread = int(sys.argv[4])
        
    else:
        print("Usage: <scheduler.py> -s <QueueName> -t <threads>")
        sys.exit(0)
    
    
    main()
    
    
