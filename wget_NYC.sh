pip3 install subsample 

mkdir -p data/yellow/yearmo
mkdir -p data/yellow/samples 
cd data/yellow

for YEAR in 2016
do
	for MONTH in 01 02 03 04 05 06 07 08 09 10 11 12
	do 
		wget -O yearmo/yellow_tripdata_$YEAR_$MONTH.csv \
		https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_$YEAR-$MONTH.csv
		
		subsample -r -n 2000 yearmo/yellow_tripdata_$YEAR_$MONTH.csv \
		> samples/yellow_sample_$YEAR_$MONTH.csv
	done 
done 

