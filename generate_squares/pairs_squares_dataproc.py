# Arif-Chuang-Scivittaro
# CMSC 123 Spring 2017
# Chicago Taxi Data Project
#
# MRJob code for Chicago Taxi Data
# Comparing path of two trips
# This is the version to use in Dataproc. 
# Usage: python3 pairs_squares_dataproc.py -r dataproc 
#   -c mrjob.conf --num-core-instances 4 
#   --file <filename.csv> <filename.csv> 

import csv
import os
from mrjob.job import MRJob
from shapely.geometry import Point
from shapely.geometry import Polygon
from generate_squares import get_squares
from  small_range import small_range


TAXI_ID = 2
TRIP_START = 3
TRIP_END = 4
PICKUP_LAT = 18
PICKUP_LONG = 19
DROP_LAT = 21
DROP_LONG = 22
PICKUP_AREA = 9
DROPOFF_AREA = 10
cwd = os.getcwd()

class MRCompare(MRJob):
    '''
    A Mapreduce class that converts pairs of trips into coordinates 
    representing the polygon surrounding their intersections and then sums up
    the counts of each coordinate associated with an intersection.
    '''
    def mapper_init(self):
        '''
        A method that initializes a copy of the data CSV file so that pairs of
        trips can be generated
        '''
        self.f = open(cwd + '/50k.csv', 'rb')
        next(self.f)
        self.bytes = self.f.tell()
        self.length = self.f.seek(0,2)


    def mapper(self, _, line):
        '''
        A mapper method that takes two trips, extracts the coordinates of the
        startpoints and endpoints, checks if two trips intersect, and is so
        find the space around the intersection and passes the coordinates and
        the value 1 to the combiner
        '''
        fields = line.split(',')

        row_number = fields[0]
        ride_id = fields[2]
        start_lat = fields[18]
        start_long = fields[19]
        end_lat = fields[21]
        end_long = fields[22]

        start = (start_lat, start_long)
        end = (end_lat, end_long)

        if (start != end and start_lat != "" and start_long != "" and end_lat
        != "" and end_long != ""):
            spot_in_file = (int(row_number) + 1) * self.bytes
            self.f.seek(spot_in_file)
            while spot_in_file < self.length:
                spot_in_file += self.bytes
                compare_line = next(self.f).decode('utf-8').strip().split(',')
                compare_id = compare_line[1]
                comp_start_lat = compare_line[18]
                comp_start_long = compare_line[19]
                comp_end_lat = compare_line[21]
                comp_end_long = compare_line[22]

                compare_start = (comp_start_lat, comp_start_long)
                compare_end = (comp_end_lat, comp_end_long)

                if (compare_start != compare_end and comp_start_lat != "" and
                comp_start_long != "" and comp_end_lat != "" and 
                comp_end_long != "" ):
                    try:
                        start = (float(start[0]), float(start[1]))
                        end = (float(end[0]), float(end[1]))
                        compare_start = (float(compare_start[0]), 
                            float(compare_start[1]))
                        compare_end = (float(compare_end[0]), 
                            float(compare_end[1]))
                        trip1 = (start, end)
                        trip2 = (compare_start, compare_end)

                        polygon_coords = get_squares(trip1, trip2)
                    except:
                        polygon_coords = None

                    if polygon_coords:
                        c1,c2,c3,c4 = polygon_coords
                        polygon = Polygon([c1,c2,c3,c4])

                        xmin = round(min(c1[0], c2[0], c3[0], c4[0]), 2)
                        xmax = round(max(c1[0], c2[0], c3[0], c4[0]), 2)
                        ymin = round(min(c1[1], c2[1], c3[1], c4[1]), 2)
                        ymax = round(min(c1[1], c2[1], c3[1], c4[1]), 2)

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

    '''
    def reducer_init(self):
        
        A reducer_init method that opens a CSV file, which will contain the
        final unqiue coordinates and associated counts
        
        self.f = open("/home/student/CS123-Project/generate_squares/coor_counts.csv", 'w')
        self.w = csv.writer(self.f)
        self.w.writerow(["Degrees Latitude", "Degrees Longitude", "Density"])

    '''

    def reducer(self, coor, counts):
        '''
        A reducer method that increments the counts of each unique grid 
        coordinate and outputs the final counts.
        Inputs:
            coor: A coordinate representing one square in a grid
        Yields:
            counts: The counts associated with each square in the grid
        '''
        #self.w.writerow([coor[0], coor[1], sum(counts)])

        yield (coor[0], coor[1]), sum(counts)


if __name__ == '__main__':

    MRCompare.run()



