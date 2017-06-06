# Arif-Chuang-Scivittaro
# CMSC 123 Spring 2017
# Chicago Taxi Data Project
#
# Quick script to process heatmap dataproc output
# from .tsv to desired .csv formatting. 
#
# Usage: python3 postprocess.py </path/to/output.tsv>

import csv
import sys

def postprocess(file):
	'''
	file: must be full path to file. 
		ex: ~/Desktop/project/output.tsv
	'''


	assert file[-3:] == 'tsv'

	with open(file, 'r') as f, open(file[0:-4] + '_processed.csv', 'w') as out:

		w = csv.writer(out)
		w.writerow(['Degrees Latitude', 'Degrees Longitude', 'Density'])
		for line in f:
			items = line.split('\t')
			density = items[1].strip()
			lat = items[0].split(',')[0][1:]
			lon = items[0].split(',')[1][1:-1]
			w.writerow([lat, lon, density])




if __name__ == '__main__':

	file = sys.argv[1]
	postprocess(file)