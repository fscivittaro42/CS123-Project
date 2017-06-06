# Arif-Chuang-Scivittaro
# CMSC 123 Spring 2017
# Chicago Taxi Data Project
#
# MRJob script to calculate the average fare
# and tip as a percent of fare, aggregated by month. 

from mrjob.job import MRJob 

TAXI_ID = 2
TRIP_START = 3
TRIP_END = 4
PICKUP_LAT = 18
PICKUP_LONG = 19
DROP_LAT = 21
DROP_LONG = 22
PICKUP_AREA = 9
DROPOFF_AREA = 10
FARE = 11
TIPS = 12

class monthlyavgfare(MRJob):

	def mapper(self, _, line):

		cols = line.split(',')
		try:
			month = int(cols[TRIP_START].split('/')[0])
			fare = float(cols[FARE].strip('$').strip())
			tip = float(cols[TIPS].strip('$').strip())
			tip_percent = tip/fare

		except:
			month = None
			fare = None
			tip = None

		if month and fare and tip:
			yield month, (fare, tip_percent)


	def reducer(self, month, fare_tippercent):
		values = list(fare_tippercent)
		fare = [i[0] for i in values]
		tip_percent = [i[1] for i in values]

		yield month, (sum(fare)/len(fare), sum(tip_percent)/len(tip_percent))



if __name__ == '__main__':
	monthlyavgfare.run()