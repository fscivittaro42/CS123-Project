# CS123-Project
CS-123 Project/utils/

Some util files for data cleaning and initializing Google Cloud instances. 

- add_row_nums.py: Adds row numbers as the first column of a csv
- add_bytes.py: Adds a column of zeros at the end of each row to make every row have the same number of bytes, in order to be able to use file.seek() to jump to a certain row
- startup_script.sh: Some startup commands for VM instances
- postprocess.py: flatten dataproc output into a csv, usable to generate heatmap