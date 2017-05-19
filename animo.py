
""" 
Animoto clone to create video stream from Image file 
it uses BOTO Python and FFMPEG library to create video 
streams 

Sunny Changediya 
""" 

import urllib 
import sys 
import string 
import subprocess 
import socket 
import json 
import boto 
from boto.s3.connection import S3Connection 
from boto.s3.key import Key 
import os 
 
# Update starting URL file name containing URL list 
workfile = "sample_image" 

# Start the server 
MY_IP = 'localhost' # Update Manually 
MY_PORT = 45003 

# Size of data to receive from remote workers 
object_size = 8192 


def process_data(url_list): 

	# Download the images 
	print("Downloading Images") 
	counter = 1 
	 
	for url_data in url_list: 
		 
		if len(str(counter)) < 2: 
			str_counter = '0'+ str(counter) 
		else: 
			str_counter = str(counter) 
		imagename = "image" + str_counter + ".jpg" 
		 
		try: 
			urllib.request.urlretrieve(url_data, imagename) 
			counter = counter+1 
		except: 
			pass 
	 
	 
	# Create a video 
	frames_per_second = 1 
	video_title = "myvideo.mp4" 
	 
	#video_title = str("my_video") + ".mp4" 
   
	FP = open("temp", 'w') # To write the output of subprocess to dev null. 
   
	subprocess.call(["ffmpeg -i","-framerate",str(frames_per_second),"-i","image%02d.jpg","-c:v", "libx264","-r", "30","-pix_fmt","yuv420p", video_title], shell= True, stdout=FP, stderr=subprocess.STDOUT) 
   
	FP.close() 

	# Connect to S3 bucket and store the video 
	print("Now Looking for AWS Connections") 
	 
	AWS_ACCESS_KEY_ID = "" #Update manually 
	AWS_SECRET_ACCESS_KEY = ""  #Update manually 

	conn = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY) 
	 
	print("Creating Bucket") 
	 
	mybucket = conn.get_bucket('ibmindia') # Update manually: bucket name 
	 
	# Get bucket object 
	key_object = Key(mybucket) 
	key_object.key = video_title 
	 
	# Get contents from Bucket stored by remote workers 
	key_object.set_contents_from_filename(video_title) 
	 
	# Save data to S3 
	saved_key = mybucket.get_key(video_title) 
	saved_key.set_canned_acl('public-read') 

	# Create public URL for that video 
	image_url = saved_key.generate_url(0, query_auth=False, force_http=True) 
	task_result = image_url 
	print("Video URL is:") 
	print(task_result) 

""" 
Function to extract tasks from JSON objects 
Task-id, task_type, and last_job are extracted 

""" 
def unpack_data(task_json_obj): 

	# Try to unpack the json object. If error, set the error message 
	converted = 0 
	 
	try: 
	   	task_object = '' 
	   	task_object = json.loads(task_json_object) 
	   	converted =1 
	except: 
	   	converted =0 
	   	task_result = "There was some error reading the request message containing the URLs and TaskID." 
	   	task_id = "temp_error_id" 
   
	return converted, task_object 


""" 
Read data file 
""" 
def read_data(): 

	with open(workfile,'r') as fp: 
		data = fp.readlines() 
	 
	return data 


""" 
Get URL data from remote workers 
and image file 
""" 
def accept_data(): 
	 
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
	server_address = (MY_IP, MY_PORT) 
	sock.bind(server_address) 
	 
	# Listen for incoming connections, receive the json object and close the connection. 
	print(" Animoto: trying to receive url lists") 

	sock.listen(1) 
	 
	last_job = False 
	 
	# get till last job is not received 
	while(last_job != True): 
	 
		connection, client_address = sock.accept() 
		task_json_object = connection.recv(object_size) 
		task_json = json.loads( task_json_object.decode('utf-8') ) 
		connection.close() 
		 
		last_job = task_json["last_batch"] 
	 
	 
	return task_json_object 


def main(): 

	#task_json_obj = accept_data() 
	data = read_data() 
	 
	flag, task_obj = unpack_data(task_json_obj) 
	 
	process_data(data) 
	 

if __name__ == '__main__': 
	main() 
