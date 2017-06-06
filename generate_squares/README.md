# CS123-Project
CS-123 Project/generate_squares

Contains the bulk of the code used to generate the congestion heatmaps. 

- results/: some initial test results and heatmaps
- helpers.zip: contains helper functions passed to dataproc

Steps:
1) Run pairs_squares_dataproc.py on dataset and pipe output to a .tsv
2) Process .tsv file with ~/utils/postprocess.py
3) Run make_heatmap.py on the resulting .csv file to create heatmap with seaborn
4) Run gmaps_heatmap.py in Jupyter Notebook on the same .csv to create heatmap with gmaps

Notes:
- pairs_squares_dataproc.py runtime for 500k subsample on 10 'n1-standard-4' nodes was approximately 8 hours