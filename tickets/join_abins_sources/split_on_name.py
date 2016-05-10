import sys
import traceback
import json

json_filename = sys.argv[1]
json_field_split = sys.argv[2]

with open(json_filename) as json_file:
    lines = json_file.readlines()

outfiles = []
names = []

if json_filename == '':
    outfiles.append( open( json_filename + '_' + 'name','w' ) )
    names.append('name')

for line in lines:
    # Ignore non json lines (apart from newline)
    if line[0]=='{':
        if json_filename != '':
            json_line = json.loads(line)
            # Open new file if name does not exist
            try:
                if not json_line['name'] in names:
                    names.append( json_line['name'] )
                    outfiles.append( open( json_filename + '_' + json_line['name'], 'w' ) )
                else:
                    json_id = names.index(json_line['name'])
                    outfiles[json_id].write(line)
            except KeyError:
                print "'name' not found in:\n" + line
                traceback.print_exc()
                sys.exit(0)
        else:
           outfiles[0].write(line) 

