# Arif-Chuang-Scivittaro
# CMSC 123 Spring 2017
# Chicago Taxi Data Project
# MRJob code for Chicago Yellow Taxi Data
# Average tip as a proportion of fare


from mrjob.job import MRJob

class TaxiAvgFare(MRJob):

    def mapper(self, _, line):
        """
        Straightforward mapper. Mapper takes fare and tip numbers
        and yields the proportion. Reducer then takes the average
        of these proportions to find the avg percentage tip. 
        """
        column = line.split(',')
        fare = column[11]
        tip = column[12]
        if fare != "Fare" and tip != "Tips":
            dfare = fare.strip("$")
            dtip = tip.strip("$")
            if dfare != "0.00" and dtip != "0.00":
               proportion = float(dtip)/float(dfare)
               yield None, proportion

    def reducer(self, _, proportion):
        proportion_list = list(proportion)
        yield None, sum(proportion_list)/len(proportion_list)


if __name__ == '__main__':
    TaxiAvgFare.run()