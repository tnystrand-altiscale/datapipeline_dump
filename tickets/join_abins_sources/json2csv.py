import json
import sys

jsonname = sys.argv[1]
csvfilename = jsonname + ".csv"

with open(jsonname) as f:
   lines = f.readlines()

csvfile = open(csvfilename,'w')

# Get headers
json_line = json.loads(lines[0])
header = [obj for obj in json_line]

print 'The header:'
print ' '.join(header)
csvfile.write( ",".join(header) + '\n' )


for line in lines:
    json_line = json.loads(line)
    curr_header = [obj for obj in json_line]
    csv_line = []
    for column in header:
        if column=='timestamp':
            #csv_line.append( str( int(json_line[column]/1000/60)*60 ) )
            csv_line.append( str( "%f" % json_line[column] ) )
        elif column=='tags':
            csv_line.append( json_line[column]['cluster'] )
        else:
            # Safe guard against schema changes
            if column in curr_header:
                csv_line.append( str( json_line[column] ) )
            else:
                csv_line.append( 'null' )

    csvfile.write(",".join( csv_line ) + '\n')
