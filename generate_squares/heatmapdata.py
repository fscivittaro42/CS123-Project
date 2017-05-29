# Francesco Scivittaro
# CS123 Final Project

import csv
from shapely.geometry import Point
from shapely.geometry import Polygon
from mrjob.job import MRJob

class MRGetHeatMaps(MRJob):
    '''
    '''
    def mapper(self, _, line):
        '''
        '''
        fields = line.split(',')
        c1 = fields[0]
        c2 = fields[1]
        c3 = fields[2]
        c4 = fields[3]

        polygon = Polygon([c1, c2, c3, c4])

        xmin = round(min(c1[0], c2[0], c3[0], c4[0]), 3)
        xmax = round(max(c1[0], c2[0], c3[0], c4[0]), 3)
        ymin = round(min(c1[1], c2[1], c3[1], c4[1]), 3)
        ymax = round(max(c1[1], c2[1], c3[1], c4[1]), 3)

        for i in small_range(xmin, xmax):
            for j in small_range(ymin, ymax):
                if polygon.intersects(Point(i,j)):
                    yield (i,j), 1

    def combiner(self, coor, counts):
        '''
        '''
        yield coor, counts

    def reducer(self, coor, counts):
        '''
        '''
        yield coor, counts

def small_range(start, stop):
    '''
    '''
    r = start
    while r < stop:
        yield r
        r += 0.001 

if __name__ == '__main__':
    MRGetHeatMaps.run()