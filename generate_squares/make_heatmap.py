# Arif-Chuang-Scivittaro
# CMSC 123 Spring 2017
# Chicago Taxi Data Project

# File that draws the heatmap with seaborn

import pandas as pd 
import matplotlib.pyplot as plt
import seaborn as sns
import math

def make_heatmap(dataset_loc):
    '''
    Makes a heatmap from a dataset containing coordinates and densities for 
    each spot on a grid representing the locations of intersections of trips

    Inputs:
        dataset_loc: The location of the CSV file containing the data
    Output:
        Saves an image of the heatmap as PNG  
    '''
    df = pd.read_csv(dataset_loc)
    df["Log of Density"] = df["Density"].map(math.log10)
    pivoted = df.pivot(index = "Degrees Latitude",
                    columns = "Degrees Longitude", values = "Log of Density")

    ax = sns.heatmap(pivoted, xticklabels = 4, yticklabels = 4)
    ax.invert_yaxis()

    plt.show()
    plt.savefig('Chicago_Trips.png')