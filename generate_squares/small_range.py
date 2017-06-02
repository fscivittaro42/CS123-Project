# File containing a helper function for generating the grid in the heat map
# CMSC 123 Final Project

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
    while r <= stop:
        yield r
        r += 0.01