# Francesco Scivittaro, Salman Arif, Andrew Chuang
# CS123 Final Project

'''
NOTE: Given the relatively small size of Chicago, the curvature of the earth
will be treated as negligible and not considered for these calculattions.
'''

from shapely.geometry import segmentString
import math

def get_squares(trip1, trip2):
    '''
    Finds the coordinates of the squares that will be overlaid onto the heat
    map if the trips intersect

    Inputs:    
        trip1: A trip from the taxi dataset
        trip2: Another trip
    Returns:
        A tuple representing the four coordinates of a square that will be
        overlaid onto the heat map
    '''
    segment1 = get_segment(trip1)
    segment2 = get_segment(trip2)

    if segment1.intersects(segment2):
        intersection = segment1.intersection(segment2)
    else:
        return None

    x = intersection.x
    y = intersection.y

    angle = get_angle(segment1, segment2, (x,y))
    scale_factor = 1 - (angle / 90)

    diag1 = get_diag_coor(segment1, scale_factor)
    diag2 = get_diag_coor(segment2, scale_factor)

    return (diag1[0], diag2[0], diag1[1], diag2[1])


def get_segment(trip):
    '''
    Generates a Shapely segment object from the two endpoints of a trip

    Inputs:
        trip: A trip from the Taxi data
    Returuns:
        A segment object using the Shapely library
    '''
    x1 = trip['Pickup Centroid Longitude']
    y1 = trip['Pickup Centroid Latitude']
    x2 = trip['Dropoff Centroid Longitude']
    y2 = trip['Dropoff Centroid Latitude']

    segment = LineString([(x1, y1), (x2, y2)])

    return segment

def get_angle(segment1, segment2, intersect):
    '''
    Finds the acute angle between two segment segments

    Inputs:
        segment1: A segment from the taxi data
        segment2: A segment from the taxi data
        intersection: The coordinate of the intersection, as a tuple
    Returns:
        A float between 0 and 90
    '''

    segment1_start = list(segment1.coords)[0]
    segment2_start = list(segment2.coords)[0]

    vector1 = (segment1start[0] - intersect[0], segment1start[1] - intersect[1])
    vector2 = (segment2start[0] - intersect[0], segment2start[1] - intersect[1])

    mag1 = math.sqrt(vector1[0]**2 + vector1[1]**2)
    mag2 = math.sqrt(vector2[0]**2 + vector2[1]**2)
    dot_product = (vector1[0] * vector2[0] + vector1[1] * vector2[1])

    cos_theta = dot_product / (mag1 * mag2)
    theta = (acos(cos_theta) * 180) / math.pi

    if theta > 90:
        theta = 180 - theta

    return theta

def get_diag_coor(segment, scale_factor, intersect):
    '''
    Finds the endpoints on a diagonal of the polygon that will be overlaid

    Inputs:
        segment: A line segment representing one route
    Returns:
        A tuple containing the coordinates of the endpoints on the diagonal
    '''
    coords = list(segment.coords)

    vector1 = (coords[0][0] - intersect[0], coords[0][1] - intersect[1])
    vector2 = (coords[1][0] - intersect[0], coords[1][1] - intersect[1])

    mag1 = math.sqrt(vector1[0]**2 + vector1[1]**2)
    mag2 = math.sqrt(vector2[0]**2 + vector2[1]**2)
    adj_mag1 = scale_factor * mag1
    adj_mag2 = scale_factor * mag2

    x1 = intersect[0] + (vector1[0] / adj_mag1)
    y1 = intersect[1] + (vector1[1] / adj_mag1)
    x2 = intersect[0] - (vector2[0] / adj_mag2)
    y2 = intersect[1] - (vector2[1] / adj_mag2)

    return ((x1, y1), (x2, y2))