# Arif-Chuang-Scivittaro
# CMSC 123 Spring 2017
# Chicago Taxi Data Project
#
# MRJob script to calculate the average fare
# and tip as a percent of fare, aggregated by (year, month)

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

class monthyr_avgfare(MRJob):

	def mapper(self, _, line):

		cols = line.split(',')
		try:
			#area = int(cols[PICKUP_AREA].strip())
			month = int(cols[TRIP_START].split('/')[0])
			year = int(cols[TRIP_START].split('/')[2][0:5].strip())
			fare = float(cols[FARE].strip('$').strip())
			tip = float(cols[TIPS].strip('$').strip())
			tip_percent = tip/fare
		
		except:
			month = None
			year = None 
			fare = None
			tip = None

		if month and year and fare and tip:
			yield (year, month), (fare, tip_percent)


	def reducer(self, monthyear, fare_tippercent):
		values = list(fare_tippercent)
		fare = [i[0] for i in values]
		tip_percent = [i[1] for i in values]

		avgfare = round((sum(fare)/len(fare)), 2)
		avgtip_percent = round(sum(tip_percent)/len(tip_percent), 2)

		yield (monthyear), (avgfare, avgtip_percent)


if __name__ == '__main__':
	monthyr_avgfare.run()