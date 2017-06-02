# File that draws the heatmap
# CS 123 Final Project

import pandas as pd 
import seaborn as sns
import matplotlib.pyplot as plt


def make_heatmap(dataset_loc):
    '''
    Makes a heatmap from a dataset containing coordinates and densities for each coordinate

    Inputs:
        dataset_loc: The location of the CSV file containing the data
    '''
    df = pd.read_csv(dataset_loc)
    pivoted = df.pivot(index = "Degrees Longitude",
                    columns = "Degrees Latitude", values = "Density")
    sns.heatmap(pivoted, xticklabels = 10, yticklabels = 10)
    plt.show()
    plt.savefig('Chicago_Trips.png')