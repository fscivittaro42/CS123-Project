# CMSC 123
# MRJob code for Chicago Taxi Data
# Comparing path of two trips


import csv
import os
from mrjob.job import MRJob
from generate_squares import get_squares
from small_range import small_range
from shapely.geometry import Point
from shapely.geometry import Polygon

TAXI_ID = 1
TRIP_START = 2
TRIP_END = 3
PICKUP_LAT = 17
PICKUP_LONG = 18
DROP_LAT = 20
DROP_LONG = 21
PICKUP_AREA = 8
DROPOFF_AREA = 9
cwd = os.getcwd()

class MRCompare(MRJob):

    def mapper_init(self):
        self.f = open(cwd + "/big_sample.csv")
        self.r = csv.reader(self.f)

    def mapper(self, _, line):
        fields = line.split(',')

        ride_id = fields[0]
        start_lat = fields[17]
        start_long = fields[18]
        end_lat = fields[20]
        end_long = fields[21]

        start = (start_lat, start_long)
        end = (end_lat, end_long)

        if (start != end and start_lat != "" and start_long != "" and end_lat
        != "" and end_long != ""):
            self.f.seek(0)
            for compare_line in self.r:
                compare_id = compare_line[0]
                comp_start_lat = compare_line[17]
                comp_start_long = compare_line[18]
                comp_end_lat = compare_line[20]
                comp_end_long = compare_line[21]

                compare_start = (comp_start_lat, comp_start_long)
                compare_end = (comp_end_lat, comp_end_long)

                if (compare_start != compare_end and compare_id != ride_id and
                comp_start_lat != "" and comp_start_long != "" and 
                comp_end_lat != "" and comp_end_long != ""):
                    start = (float(start[0]), float(start[1]))
                    end = (float(end[0]), float(end[1]))
                    compare_start = (float(compare_start[0]), 
                        float(compare_start[1]))
                    compare_end = (float(compare_end[0]), 
                        float(compare_end[1]))
                    trip1 = (start, end)
                    trip2 = (compare_start, compare_end)

                    polygon_coords = get_squares(trip1, trip2)

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


    def reducer_init(self):
        '''
        A reducer_init method that opens a CSV file, which will contain the
        final unqiue coordinates and associated counts
        '''
        self.f = open("/home/student/CS123-Project/generate_squares/coor_counts.csv", 'w')
        self.w = csv.writer(self.f)
        self.w.writerow(["Degrees Longitude", "Degrees Latitude", "Density"])


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
        print((coor[0], coor[1]))
        print(sum(counts))
        print()


if __name__ == '__main__':
    MRCompare.run()



