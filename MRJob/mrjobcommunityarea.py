# Arif-Chuang-Scivittaro
# CMSC 123 Spring 2017
# Chicago Taxi Data Project
#
# MRJob job that calculates the number of trips 
# 


from mrjob.job import MRJob

class TaxiPickDropComm(MRJob):

    def mapper(self, _, line):
    	column = line.split(',')
    	comm_pick_up = column[9]
    	comm_drop_off = column[10]
    	yield (comm_pick_up, comm_drop_off), 1

    def combiner(self, pickdrop, counts):
        yield pickdrop, sum(counts)

    def reducer(self, pickdrop, counts):
    	yield pickdrop, sum(counts)


if __name__ == '__main__':
    TaxiPickDropComm.run()