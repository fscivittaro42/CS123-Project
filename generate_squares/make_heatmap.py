# File that draws the heatmap
# CS 123 Final Project

import pandas as pd 
import matplotlib.pyplot as plt
import gmaps
import gmaps.datasets

def make_heatmap(dataset_loc):
    '''
    Makes a heatmap from a dataset containing coordinates and densities for each coordinate

    Inputs:
        dataset_loc: The location of the CSV file containing the data
    '''
    gmaps.configure(api_key="AIzaSyB_j56rOXCyrKLPhEpCqkqEs6cFvX949gA")
    df = pd.read_csv(dataset_loc)
    #pivoted = df.pivot(index = "Degrees Latitude",
    #                columns = "Degrees Longitude", values = "Density")
    #sns.heatmap(pivoted)
    #plt.show()
    #plt.savefig('Chicago_Trips.png')

    locations = df[["Degrees Latitude", "Degrees Longitude"]]
    weights = df["Density"]
    fig = gmaps.figure()
    fig.add_layer(gmaps.heatmap_layer(locations, weights=weights))
    fig