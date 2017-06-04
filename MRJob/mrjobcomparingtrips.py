# CMSC 123
# Test MRJob code for NYC Yellow Taxi Data
# Comparing path of two trips
#########################################################################
# Code ultimately unused for actual project because better method was
# found. This code took too much time to compare pairs on a 2k subsample
# of the code indicated that it would be unrealistic to use for the 
# full dataset. 
########################################################################


from mrjob.job import MRJob
import csv	

CSVFILE = open("yellow2016_1_subsample.csv", 'r')
TAXI_READER = csv.reader(CSVFILE)


class TaxiTripComparison(MRJob):
    
    def mapper(self, _, line):
        """MRJob class meant to be run on the csv output
        by enumerate.py. Enumerate.csv contained two columns
        generated through a nested pair of for loops."""
        
        trips = line.split(',')
        first_row = int(trips[0])
        second_row = int(trips[1])
        
        # Following loops used to 
        #
        
        CSVFILE.seek(0)
        for i in range(first_row + 1):
        	if i == first_row:
        		row1_result = next(TAXI_READER)
        	else:
        		next(TAXI_READER)

        CSVFILE.seek(0)
        for i in range(second_row):     
            row2_result = next(TAXI_READER)

        one_start_coord = (row1_result[5], row1_result[6])
        one_end_coord = (row1_result[9], row1_result[10])

        two_start_coord = (row2_result[5], row2_result[6])
        two_end_coord = (row2_result[9], row2_result[10])
        
        print(two_start_coord)

        yield None, ((one_start_coord, one_end_coord), (two_start_coord, two_end_coord))



if __name__ == '__main__':
    TaxiTripComparison.run()



