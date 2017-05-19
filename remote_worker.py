"""
Remote workers code to fetch data from SQS Request Queue
and submit response back to SQS. Uses Python BOTO3 library

Sunny Changediya

"""

import time
import json
import sys
from multiprocessing import Process
from collections import defaultdict
import boto3
from boto3.dynamodb.conditions import Key, Attr

def dynamo_table_check(batch):
	
	flag = 0
	
	res = boto3.resource('dynamodb')
	handle = res.Table('TaskUsers')
	
	print(batch)
	print(handle)
	
	task_id = batch["id"]
	#task_cnt = int( batch["id"].split('_')[-1] )
	
	# Check whether task already exists
	result = handle.query(KeyConditionExpression=Key('file_id').eq(batch["id"]) )
	
	if result['Count'] == 0: #item not found, table.put() 
		handle.put_item(Item={'file_id': batch["id"]})
		flag = 1
	else:
		# Item found...
		flag = -1
	
	return flag
	
def request_tasks():
	
	Req_queue = 'RequestQueue'
	
	unique_tasks = []
	duplicates = []
	
	total_msg_received = 0
	
	# Get the service resource
	sqs = boto3.resource('sqs')
	# Get the queue instance
	
	found = False
	
	while found != True:
		
		try:
			queue = sqs.get_queue_by_name(QueueName='RequestQueue')
			found = True	
		except:
			continue
		
	#queue = sqs.get_queue_by_name(QueueName='RequestQueue')
	
	# DYnamoDB Table
	resource = boto3.resource('dynamodb')
	client = boto3.client('dynamodb')
	
	found = False
	# Get DynamoDB Table Handle
	while found != True:
		
		try: # Check for table already existence
			tabledesc = client.describe_table(TableName='TaskUsers')
			table = resource.Table('TaskUsers')
			found = True
		except:
			continue
			#print("DynamoDB table does not exists")
			#sys.exit(0)
	
	
	que = sqs.Queue(queue.url)
	
	done = False
	print("Remote worker Sleeping")
	time.sleep(10)
	while (done != True):
		
		# it returns a list, so msg is a list
		msg = queue.receive_messages()
		
		if msg:
			
			batch = json.loads(msg[0].body)
			flag = dynamo_table_check(batch)
			
			# Check for entry already in a table
			if flag == 1:
			
		   		# Entry added to table and now process the message
		   		msg[0].delete()
		   		sleep_val = int(batch["data"].split()[1])
		   		time.sleep(sleep_val)
		   		print("RMW: sleep done")
		   		unique_tasks.append(batch)
		   		
			else: # task entry already there
				duplicates.append(batch)
				pass
		   		# pass or continue
		else:
			done = True

	print("RMW: messages got from RequestQueue")
	print("Unique tasks executed %d & Duplicates are %d" %(len(unique_tasks), len(duplicates)))
	
	return unique_tasks
	

def response_task(real_task_list):

	queue_name = 'ResponseQueue'
			
	# Get the 'sqs' service resource
	sqs = boto3.resource('sqs')
	
	# get Response Queue handle, from already created Queue
	my_queue = sqs.get_queue_by_name(QueueName='ResponseQueue')
	
	"""
	# Create SQS Queue
	my_queue = sqs.create_queue(QueueName=queue_name, Attributes={'DelaySeconds': '0'})
	"""
	
	# Get Queue URL
	queueurl = my_queue.url
	print("Response Queue URL is %s" %(queueurl) )
   
	# Send json_obj directly to sqs.send_message()
	# DO not include "encode", otherwise convert it to str(json.encode) 
	# and send. It will add extra bytes to sqs
    
	for i in range(len(real_task_list)):
   	
		json_obj = json.dumps(real_task_list[i])
		#json_batch = json_obj.encode('utf-8')
  	
		response = my_queue.send_message(MessageBody=json_obj)
	###

def main():
	
	# get data from Response Queue
	real_task_list = request_tasks()
	
	# submit those tasks to Response Queue
	response_task(real_task_list)
	
	print("All Remote Responses submitted")
	
	
if __name__ == '__main__':
    
    
    if len(sys.argv) == 5 and sys.argv[1] == '-s' and sys.argv[3] == '-w':
        
        #scheduler_ip = sys.argv[2].split(':')[0]
        #scheduler_port = int(sys.argv[2].split(':')[1])
        no_of_threads = int(sys.argv[4])
        
    else:
        print("Usage: <remote_worker.py> -s <QueueName> -t <threads>")
        sys.exit(0)
    
    
    main()
