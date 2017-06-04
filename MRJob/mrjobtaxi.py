# CMSC 123
# Test MRJob code for NYC Yellow Taxi Data
#############################################
# Unused in final project because Chicago 
# dataset did not provide passenger count
# statistics. 
#############################################


from mrjob.job import MRJob


class TaxiRiderMax(MRJob):

    def mapper(self, _, line):
        column_var = line.split(',')
        rider_count = column_var[4]
        yield None, max(rider_count)

    def combiner(self, _, count):
        yield None, max(count)

    def reducer(self, _, count):
        yield None, max(count)

if __name__ == '__main__':
    TaxiRiderMax.run()
