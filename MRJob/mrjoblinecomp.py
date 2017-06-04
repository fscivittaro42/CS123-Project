# Arif-Chuang-Scivittaro
# CMSC 123 Spring 2017
# Chicago Taxi Data Project
#
# Comparison between Line 1 and all other lines
# Average tip as a proportion of fare


from mrjob.job import MRJob

TAXIDATA = open("yellow2016subsample.csv")

class TaxiRouteComp(MRJob):

    def mapper(self, _, line):
    	column = line.split(',')
    	comp_start = TAXIDATA[column[1]][5]
        comp_end = TAXIDATA[column[1]][6]
    	yield None, (comp_start, comp_end)

    def reducer(self, _, comp):
    	yield None, comp


if __name__ == '__main__':
    TaxiRouteComp.run()