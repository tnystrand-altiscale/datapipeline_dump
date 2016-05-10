import sys
import json

json_filename = sys.argv[1]

with open(json_filename) as json_file:
    lines = json_file.readlines()

outfile = open( json_filename + '_' + 'processed', 'w' ) 

for line in lines:
    # Ignore non json lines (apart from newline)
    if line[0]=='{':
       outfile.write(line) 

