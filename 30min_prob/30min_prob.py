# Arif-Chuang-Scivittaro
# CMSC 123 Spring 2017
# Chicago Taxi Data Project
#
# Python MRJob code to calculate 
# the likelihood of a driver's next pickup being found 
# within 30 minutes, probabilites 
# aggregated by neighborhood. 
# 
# Local Usage: python3 30min_prob.py <filename.csv> 
# Dataproc usage: python3 30min_prob.py -r dataproc
# 	-c mrjob.conf  --num-core-instances 4
#	<chicago_data_filename.csv>
#
# Note: data file should be pre-processed with add_row_nums and add_bytes


import time
from datetime import datetime
import mrjob
from mrjob.job import MRJob
from mrjob.step import MRStep


TAXI_ID = 2
TRIP_START = 3
TRIP_END = 4
PICKUP_LAT = 18
PICKUP_LONG = 19
DROP_LAT = 21
DROP_LONG = 22
PICKUP_AREA = 9
DROPOFF_AREA = 10

class MRlikelihood(MRJob):

	#OUTPUT_PROTOCOL = mrjob.protocol.JSONValueProtocol
	#MRJob.HADOOP_OUTPUT_FORMAT = 'textOutputFormat.separatorText', ','
	#MRJob.hadoop_output_format('textOutputFormat')

	#MRJob.JOBCONF = {'mapred.tasktracker.reduce.tasks.maximum': '1', \
		#'mapreduce.job.reduces':'1'}
	
	def mapper_init(self):
		# Formatting for times in data file
		self.in_fmt = '%I:%M:%S %p'
		self.out_fmt = '%H:%M'
	

	def mapper(self, _, line):
		'''
		Constructs the 'route' for every (cab, date) pair. 
		'''
		headers = line.split(',')
		taxi = headers[TAXI_ID]
		end_area = headers[DROPOFF_AREA]
		start_day = headers[TRIP_START][0:10]
		
		try:
			# Re-format time from 12-hour to 24-hour
			start_time = time.strptime(headers[TRIP_START][10:].strip(), 
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

		if taxi and start_day and pickup[0] and dropoff[0] and dropoff[1]:	
			yield (taxi, start_day), (pickup, dropoff) 


	def reducer_init(self):
		self.fmt = '%H:%M'


	def reducer1(self, cab_day, trips):
		'''
		Takes in a list of trips for a (cab, date) and 
		yields whether or not each dropoff was followed
		by another pickup within 30 minutes, aggregated
		by dropoff area. 
		'''
		trips = list(trips)

		for trip_num in range(0, len(trips)-1):
			
			start_next = trips[trip_num+1][0][0]
			end_current = trips[trip_num][1][0]
			end_area = trips[trip_num][1][1]
			
			# Find time to next pickup 
			tdelta = datetime.strptime(start_next, self.fmt) - \
			datetime.strptime(end_current, self.fmt)
				
			
			got_under_30 = 1 * (tdelta.seconds/60 <= 30)
			
			yield int(end_area), (got_under_30, 1)

	
	def reducer2(self, area, results):
		'''
		Takes in the results from reducer1 and finds the 
		probabilities of finding next ride within 30 minutes, 
		aggregated by community area (neighborhood). 
		'''
		results = list(results)
		got_within_30 = sum(i[0] for i in results)
		total_dropoffs = sum(i[1] for i in results)

		yield area, got_within_30/total_dropoffs
		

	def steps(self):
		return [
		  MRStep(mapper_init=self.mapper_init,
				mapper=self.mapper,
				reducer_init=self.reducer_init,
				reducer=self.reducer1),
		  MRStep(reducer=self.reducer2)
		]
	

#################################################################


if __name__ == '__main__':
	MRlikelihood.run()


