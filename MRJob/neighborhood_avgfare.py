# Arif-Chuang-Scivittaro
# CMSC 123 Spring 2017
# Chicago Taxi Data Project
#
# MRJob script to calculate the average fare
# and tip as a percent of fare, aggregated by pickup neighborhood

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

class nbhdavgfare(MRJob):

	def mapper(self, _, line):

		cols = line.split(',')
		try:
			area = int(cols[PICKUP_AREA].strip())
			fare = float(cols[FARE].strip('$').strip())
			tip = float(cols[TIPS].strip('$').strip())
			tip_percent = tip/fare

		except:
			area = None
			fare = None
			tip = None

		if area and fare and tip:
			yield area, (fare, tip_percent)


	def reducer(self, area, fare_tippercent):
		values = list(fare_tippercent)
		fare = [i[0] for i in values]
		tip_percent = [i[1] for i in values]

		avgfare = round((sum(fare)/len(fare)), 2)
		avgtip_percent = round(sum(tip_percent)/len(tip_percent), 2)

		yield area, (avgfare, avgtip_percent)



if __name__ == '__main__':
	nbhdavgfare.run()