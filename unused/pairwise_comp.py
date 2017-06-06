# Arif-Chuang-Scivittaro
# CMSC 123 Spring 2017
# Chicago Taxi Data Project
#
# Exploratory script for pairwise comparisons. 
# Unused in final project. 


TAXI_ID = 1
TRIP_START = 2
TRIP_END = 3
PICKUP_LAT = 17
PICKUP_LONG = 18
DROP_LAT = 20
DROP_LONG = 21
PICKUP_AREA = 8
DROPOFF_AREA = 9

from mrjob.job import MRJob
import csv	
import os
cwd = os.getcwd()

class MRCompare(MRJob):

	def mapper_init(self):
		self.f = open(cwd + "/2k.csv")
		self.r = csv.reader(self.f)

	def mapper(self, _, line):
		fields = line.split(',')

		ride_id = fields[0]
		start_lat = fields[17]
		start_long = fields[18]
		end_lat = fields[20]
		end_long = fields[21]

		start = (start_lat, start_long)
		end = (end_lat, end_long)

		self.f.seek(0)
		for compare_line in self.r:
			compare_id = compare_line[0]
			comp_start_lat = compare_line[17]
			comp_start_long = compare_line[18]
			comp_end_lat = compare_line[20]
			comp_end_long = compare_line[21]

			compare_start = (comp_start_lat, comp_start_long)
			compare_end = (comp_end_lat, comp_end_long)

			if compare_id != ride_id:
				if comp_start_lat != "" and comp_start_long != "":
					if comp_end_lat != "" and comp_end_long != "":
						if start_lat != "" and start_long != "":
							if end_lat != "" and end_long != "":
								yield (start, end), (compare_start, compare_end)



if __name__ == '__main__':
	MRCompare.run()

