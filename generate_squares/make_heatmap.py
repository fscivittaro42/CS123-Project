# File that draws the heatmap
# CS 123 Final Project

import pandas as pd 
import matplotlib.pyplot as plt
import seaborn as sns

def make_heatmap(dataset_loc):
    '''
    Makes a heatmap from a dataset containing coordinates and densities for each coordinate

    Inputs:
        dataset_loc: The location of the CSV file containing the data
    '''
    df = pd.read_csv(dataset_loc)
    pivoted = df.pivot(index = "Degrees Latitude",
                    columns = "Degrees Longitude", values = "Density")
    ax = sns.heatmap(pivoted, xticklabels = 2, yticklabels = 2)
    ax.invert_xaxis()
    plt.show()
    plt.savefig('Chicago_Trips.png')