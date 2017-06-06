# Arif-Chuang-Scivittaro
# CMSC 123 Spring 2017
# Chicago Taxi Data Project

from mrjob.job import MRJob

class TaxiUniqueTrips(MRJob):

    def mapper(self, _, line):
        column = line.split(',')
        taxi_ID = column[2]
        yield taxi_ID, 1

    def combiner(self, taxi, trip):
        yield taxi, sum(trip)

    def reducer(self, taxi, trips):
        yield taxi, sum(trips)


if __name__ == '__main__':
    TaxiUniqueTrips.run()