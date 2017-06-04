# Arif-Chuang-Scivittaro
# CMSC 123 Spring 2017
# Chicago Taxi Data Project
#
# Python MapReduce code to obtain information on 
# the likelihood of a driver's next pickup being found 
# within 30 minutes, aggregated by neighborhood. 
# 
# Usage: python3 30min_prob.py <filename.csv> 
# Dataproc usage: python3 30min_prob.py -r dataproc
# 	-c mrjob.conf 


import time
from datetime import datetime
import mrjob
from mrjob.job import MRJob
from mrjob.step import MRStep
#import os
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

class MRlikelihood(MRJob):


	#OUTPUT_PROTOCOL = mrjob.protocol.JSONValueProtocol
	#MRJob.HADOOP_OUTPUT_FORMAT = 'textOutputFormat.separatorText', ','
	#MRJob.hadoop_output_format('textOutputFormat')

	#MRJob.JOBCONF = {'mapred.tasktracker.reduce.tasks.maximum': '1', \
		#'mapreduce.job.reduces':'1'}
	
	def mapper_init(self):
		self.in_fmt = '%I:%M:%S %p'
		self.out_fmt = '%H:%M'
	

	def mapper(self, _, line):

		headers = line.split(',')
		taxi = headers[TAXI_ID]
		end_area = headers[DROPOFF_AREA]
		start_day = headers[TRIP_START][0:10]
		
		try:
			start_time = time.strptime(headers[TRIP_START][10:].strip() , 
				self.in_fmt)
			start_time = time.strftime(self.out_fmt, start_time)
			end_time = time.strptime(headers[TRIP_END][10:].strip(), 
				self.in_fmt)
			end_time = time.strftime(self.out_fmt, end_time)

		
		except:
			start_time = None
			end_time = None


		pickup = (start_time, )
		dropoff = (end_time, end_area)

		if taxi and start_day and pickup[0]  \
			and dropoff[0] and dropoff[1]:
			yield (taxi, start_day), (pickup, dropoff) 

	'''
	def combiner(self, cab_day, trips):
	'''

	def reducer_init(self):
		self.fmt = '%H:%M'


	def reducer1(self, cab_day, trips):
		trips = list(trips)

		for trip_num in range(0, len(trips)-1):
			start_j = trips[trip_num+1][0][0]
			end_i = trips[trip_num][1][0]
			end_area = trips[trip_num + 1][1][1]
			

			tdelta = datetime.strptime(end_i, self.fmt) - \
				datetime.strptime(start_j, self.fmt)
			
			got_under_30 = 1 * (tdelta.seconds/60 <= 30)
			yield int(end_area), (got_under_30, 1)

	
	def reducer2(self, area, results):
		results = list(results)
		num = sum(i[0] for i in results)
		den = sum(i[1] for i in results)

		yield area, num/den
		

	def steps(self):
		return [
		  MRStep(mapper_init=self.mapper_init,
				mapper=self.mapper,
				reducer_init=self.reducer_init,
				reducer=self.reducer1),
		  MRStep(reducer=self.reducer2)
		]
	


if __name__ == '__main__':
	MRlikelihood.run()


