# Arif-Chuang-Scivittaro
# CMSC 123 Spring 2017
# Chicago Taxi Data Project
#
# Exploratory code to test some things with MapReduce and Dataproc.
# Unused in final project. 

import time
import mrjob
from mrjob.job import MRJob
import os
#cwd = os.getcwd()

TAXI_ID = 1
TRIP_START = 2
TRIP_END = 3
PICKUP_LAT = 17
PICKUP_LONG = 18
DROP_LAT = 20
DROP_LONG = 21
PICKUP_AREA = 8
DROPOFF_AREA = 9

FIL = open("/mnt/storage/out2.csv", 'w')
W = csv.writer(FIL)


class MRrides(MRJob):


	#OUTPUT_PROTOCOL = mrjob.protocol.JSONValueProtocol
	#MRJob.HADOOP_OUTPUT_FORMAT = 'textOutputFormat.separatorText', ','
	#MRJob.hadoop_output_format('textOutputFormat')

	MRJob.JOBCONF = {'mapred.tasktracker.reduce.tasks.maximum': '1', \
		'mapreduce.job.reduces':'1'}
	

	def mapper(self, _, line):

		headers = line.split(',')

		taxi = headers[TAXI_ID]
		start_day = headers[TRIP_START][0:10]
		try:
			start_time = time.strptime(headers[TRIP_START][10:].strip() , 
				'%I:%M:%S %p')
			start_time = time.strftime('%H:%M', start_time)
			end_time = time.strptime(headers[TRIP_END][10:].strip(), 
				'%I:%M:%S %p')
			end_time = time.strftime('%H:%M', end_time)

		except:
			start_time = None
			end_time = None

		pickup = (start_time, headers[PICKUP_LAT], headers[PICKUP_LONG])
		dropoff = (end_time, headers[DROP_LAT], headers[DROP_LONG])

		if taxi and start_day and pickup[0] and pickup[1]  \
			and dropoff[1] and pickup[2] and dropoff[2]:
			#yield (taxi, start_day), (pickup, dropoff) 
			pass 

		if taxi:
			yield taxi, 1

	'''
	def combiner(self, cab_day, trips):
	'''

	#def reducer_init(self):
		#self.f = open("/mnt/storage/out.csv", 'w')
		#self.w = csv.writer(self.f)


	def reducer(self, cab_day, trips):
		#trips = list(trips)
		trips = sum(trips)
		#self.w.writerow((cab_day, trips))
		W.writerow((cab_day, trips))
		yield cab_day, trips

		#out = [cab_day, sum(trips)]
		#yield cab_day, sum(trips)

		#if len(trips) > 1:
			#self.w.writerow((cab_day, trips))
			#yield cab_day, trips

	



if __name__ == '__main__':
	MRrides.run()


