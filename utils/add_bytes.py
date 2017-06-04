# Arif-Chuang-Scivittaro
# CMSC 123 Spring 2017
# Chicago Taxi Data Project
#
# Quick script to add a column of zeros to each row in a csv
# so that all rows have the same number of bytes. 
# Allows us to use file.seek() to jump to desired row. 

import csv
import sys

def add_bytes(file, testing):
	'''
	file: must be full path to file. 
		ex: ~/Desktop/project/data.csv 
	'''

	assert file[-3:] == 'csv'
	
	# First get the max line size
	with open(file, 'rb') as f:

		g = open(file, 'r')
		r = csv.reader(g)
		prev_posn = 0
		max_length = 0
		current_posn = 0

		f.seek(0)
		for line in r:
			prev_posn = f.tell()

			try:
				next(f)
			except:
				break
			
			current_posn = f.tell()
			length = current_posn - prev_posn

			if length > max_length:
				max_length = length
		g.close()
		

	# Now make a column of zeros to make all rows same size

	with open(file, 'rb') as f, open(file[0:-4] + '_bytes.csv', 'w') as out:
		g = open(file, 'r')
		r = csv.reader(g)
		w = csv.writer(out)

		f.seek(0)
		prev_posn = 0
		current_posn = 0
		col = 25      # first empty column

		for line in r:

			prev_posn = f.tell()

			try:
				next(f)
			except:
				break

			current_posn = f.tell()

			line_size = current_posn - prev_posn

			diff = max_length - line_size

			assert diff >= 0

			w.writerow([i for i in line] + ['0'*diff])

		g.close()

	# If we want to double check, use -test flag when running

	if (testing):
		with open(file[0:-4] + '_bytes.csv', 'rb') as f:

			g = open(file[0:-4] + '_bytes.csv', 'r')
			r = csv.reader(g)

			max_length = 0
			current_posn = 0

			next(f)
			diff = f.tell()

			f.seek(0)
			for line in r:
				prev_posn = f.tell()

				try:
					next(f)
				except:
					break
				
				current_posn = f.tell()

				assert diff == current_posn - prev_posn
		print("Tested all rows")




if __name__ == '__main__':

	file = sys.argv[1]

	testing = False
	if len(sys.argv) == 3:
		if sys.argv[2] == '-test':
			testing = True


	add_bytes(file, testing)