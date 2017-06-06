# Arif-Chuang-Scivittaro
# CMSC 123 Spring 2017
# Chicago Taxi Data Project
#
# MRJob job that calculates the payment types used by 
# passengers to pay for their taxi fares


from mrjob.job import MRJob

class TaxiPaymentType(MRJob):

    def mapper(self, _, line):
        column = line.split(',')
        payment = column[16]
        if payment != "Payment Type":
            yield payment, 1

    def combiner(self, type, count):
        yield type, sum(count)

    def reducer(self, type, count):
        yield type, sum(count)


if __name__ == '__main__':
    TaxiPaymentType.run()