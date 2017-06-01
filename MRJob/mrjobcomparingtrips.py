# CMSC 123
# Test MRJob code for NYC Yellow Taxi Data
# Comparing path of two trips


from mrjob.job import MRJob
import csv	

CSVFILE = open("yellow2016_1_subsample.csv", 'r')
TAXI_READER = csv.reader(CSVFILE)


class TaxiTripComparison(MRJob):
    
    def mapper(self, _, line):
        trips = line.split(',')
        first_row = int(trips[0])
        second_row = int(trips[1])
        
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

        """start_x_2 = row2_result[5]
        start_y_2 = row2_result[6]
        end_x_2 = row2_result[9]
        end_y_2 = row2_result[10]"""
        print(two_start_coord)

        yield None, ((one_start_coord, one_end_coord), (two_start_coord, two_end_coord))



if __name__ == '__main__':
    TaxiTripComparison.run()



