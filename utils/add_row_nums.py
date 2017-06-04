# Arif-Chuang-Scivittaro
# CMSC 123 Spring 2017
# Chicago Taxi Data Project
#
# Quick script to add row numbers to the front of each row
# in a data csv file. 
#
# Usage: python3 add_row_nums.py </path/to/file.csv>


import csv
import sys

def add_row_nums(file):

	'''
	file: must be full path to file. 
		ex: ~/Desktop/project/data.csv 
	'''

	i = 0
	assert file[-3:] == 'csv'

	with open(file, 'r') as f, open(file[0:-4] + '_nums.csv', 'w') as out:
		
		w = csv.writer(out)
		r = csv.reader(f)
		for line in r:
			w.writerow([i] + [col for col in line])
			i+=1



if __name__ == '__main__':

	file = sys.argv[1]
	add_row_nums(file)