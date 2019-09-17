filename = "missing_fsimage-csv.log"
runfile = filename + ".cmds.sh"

with open(filename, 'r') as input, open(runfile, 'r') as output:
    for entry in input:
        print entry
    
