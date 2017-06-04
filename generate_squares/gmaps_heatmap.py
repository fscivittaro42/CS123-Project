# File for making heatmaps overlayed onto Google Earth
# Scivittaro, Huang, Arif

import gmaps
import pandas as pd

def make_heatmap(location):
    '''
    A function that given the location of a CSV file makes a heatmap and 
    overlays it over a map

    Inputs:
        location: The location and filename of a CSV file containing 
        coordinates representing spots on a heatmap grid and the weights
        associated with each coordinate
    Outputs:
        A heatmaps figure in Jupyter
    '''
    gmaps.configure(api_key = "AIzaSyB_j56rOXCyrKLPhEpCqkqEs6cFvX949gA")

    df = pd.read_csv(location)
    locations = df[["Degrees Latitude", "Degrees Longitude"]]
    density = df["Density"]

    fig = gmaps.figure()
    fig.add_layer(gmaps.heatmap_layer(locations, weights = density))
    fig
