# File that draws the heatmap
# CS 123 Final Project

import pandas as pd 
import seaborn as sns
import matplotlib.pyplot as plt
import gmaps


def make_heatmap(dataset_loc):
    '''
    Makes a heatmap from a dataset containing coordinates and densities for each coordinate

    Inputs:
        dataset_loc: The location of the CSV file containing the data
    '''
    df = pd.read_csv(dataset_loc)
    #pivoted = df.pivot(index = "Degrees Latitude",
    #                columns = "Degrees Longitude", values = "Density")
    #sns.heatmap(pivoted)
    #plt.show()
    #plt.savefig('Chicago_Trips.png')

    length = len(df)
    lat = list(df["Degrees Latitude"])
    lon = list(df["Degrees Longitude"])
    density = list(df["Density"])

    gmap = gmplot.GoogleMapPlotter(41.8781, 87.6298, 16)

    gmap.heatmap(lat, lon)

    gmap.draw("mymap.html")