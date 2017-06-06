# Arif-Chuang-Scivittaro
# CMSC 123 Spring 2017
# Chicago Taxi Data Project
#
# MRJob code that tests the length of the ride in seconds

from mrjob.job import MRJob

class TaxiAvgRideLength(MRJob):

    def mapper(self, _, line):
        column = line.split(',')
        length = column[5]
        if length != "Trip Seconds" and length != "Trip End Timestamp" and length != "":
            length = float(length)
            yield None, length

    def reducer(self, _, length):
        length_list = list(length)
        yield None, sum(length_list)/len(length_list)


if __name__ == '__main__':
    TaxiAvgRideLength.run()