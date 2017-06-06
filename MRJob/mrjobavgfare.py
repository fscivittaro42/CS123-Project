# Arif-Chuang-Scivittaro
# CMSC 123 Spring 2017
# Chicago Taxi Data Project
#
# MRJob code for Chicago Taxi Data
# Average fare


from mrjob.job import MRJob

class TaxiAvgFare(MRJob):

    def mapper(self, _, line):
        """
        Straightforward mapper. Mapper takes fare and reducer takes 
        the avg of these fares. Reducer then takes the average
        of these proportions to find the avg percentage tip. 
        """
        column = line.split(',')
        fare = column[11]
        if fare != "Fare":
            dfare = float(fare.strip("$"))
            if dfare != "0.00":
               yield None, dfare

    def reducer(self, _, fare):
        fare_list = list(fare)
        yield None, sum(fare_list)/len(fare_list)


if __name__ == '__main__':
    TaxiAvgFare.run()