# Francesco Scivittaro
# CS123 Final Project

import csv
from shapely.geometry import Point
from shapely.geometry import Polygon
from mrjob.job import MRJob

class MRGetHeatMaps(MRJob):
    '''
    A mapreduce class that inherits properties from the MRJob class. This
    class will mapreduce coordinates representing the corners of a polygon and
    output each unique coordinate in each polygon along with the count 
    associated with each coordinate, where the polygon represents the 
    approximate location of where a pair of taxi trips could coincide. 
    '''

    def mapper(self, _, line):
        '''
        A mapper method that reads the lines of a CSV file containing the data
        representing a polygon and passes the coordinates representing a grid
        inside the polygon. 
        '''
        fields = line.split(',')
        print(fields)
        c1 = (float(fields[0]), float(fields[1]))
        c2 = (float(fields[2]), float(fields[3]))
        c3 = (float(fields[4]), float(fields[5]))
        c4 = (float(fields[6]), float(fields[7]))

        polygon = Polygon([c1,c2,c3,c4])

        xmin = round(min(c1[0], c2[0], c3[0], c4[0]), 2)
        xmax = round(max(c1[0], c2[0], c3[0], c4[0]), 2)
        ymin = round(min(c1[1], c2[1], c3[1], c4[1]), 2)
        ymax = round(max(c1[1], c2[1], c3[1], c4[1]), 2)

        for i in small_range(xmin, xmax):
            for j in small_range(ymin, ymax):
                if polygon.intersects(Point(i,j)):
                    yield (round(i,2),round(j,2)), 1

    
    def combiner(self, coor, counts):
        '''
        A combiner method that increments the counts of each unique grid
        coordinate.
        Inputs:
            coor: A coordinate representing one square in a grid
        Yields:
            counts: The counts associated with each square in the grid
        '''
        yield coor, sum(counts)

    def reducer_init(self):
        self.f = open("/home/student/CS123-Project/generate_squares/coor_counts.csv", 'w')
        self.w = csv.writer(self.f)

    def reducer(self, coor, counts):
        '''
        A reducer method that increments the counts of each unique grid 
        coordinate and outputs the final counts.
        Inputs:
            coor: A coordinate representing one square in a grid
        Yields:
            counts: The counts associated with each square in the grid
        '''
        self.w.writerow([coor[0], coor[1], sum(counts)])

def small_range(start, stop):
    '''
    A helper function that allows the mapper method to iterate over the grids
    in the squares. This helper function results in each grid in the square
    being 0.001 degrees of longitude by 0.001 degrees of latitude.

    Inputs:
        start: where the iteration should start
        stop: where the interation should end
    Yields:
        r: the length of the segment in pieces of width 0.001 degrees
    '''
    r = start
    while r < stop:
        yield r
        r += 0.01

if __name__ == '__main__':
    MRGetHeatMaps.run()