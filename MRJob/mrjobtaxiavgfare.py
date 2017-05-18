# CMSC 123
# Test MRJob code for NYC Yellow Taxi Data
# Average tip as a proportion of fare


from mrjob.job import MRJob

class TaxiAvgFare(MRJob):

    def mapper(self, _, line):
    	column = line.split(',')
    	fare = float(column[12])
    	tip = float(column[15])
    	if fare != 0 and tip != 0:
    	    proportion = float(tip)/float(fare)
    	    yield None, proportion

    def reducer(self, _, proportion):
    	proportion_list = list(proportion)
    	yield None, sum(proportion_list)/len(proportion_list)


if __name__ == '__main__':
    TaxiAvgFare.run()