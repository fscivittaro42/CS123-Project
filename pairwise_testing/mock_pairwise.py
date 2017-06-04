from mrjob.job import MRJob
import csv	
import os
cwd = os.getcwd()

class MRcompare(MRJob):

	def mapper_init(self):
		self.f = open(cwd + "/mock_data.csv", 'r')
		self.r = csv.reader(self.f)
		


	def mapper(self, _, line):
		fields = line.split(',')

		ride_id = fields[0]
		start = fields[1]
		end = fields[2]



		
		self.f.seek(0)

		for compare_line in self.r:

			compare_id = compare_line[0]
			compare_start = compare_line[1]
			compare_end = compare_line[2]

			if compare_id != ride_id:
				yield (start, end), (compare_start, compare_end)
		

if __name__ == '__main__':
	MRcompare.run()


