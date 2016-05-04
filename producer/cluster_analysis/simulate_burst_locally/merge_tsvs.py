import sys

def main():
    base_file = open(sys.args[0])
    join_file = open(sys.args[1])
    out_file = open('merged.tsv','w')

    base_lines = base_file.readlines()
    join_lines = join_file.readlines()
    pointer = 0

    for base in base_lines:
        
        base_split = base.split()
        minute_start = int(base_split[0])
       
        # Assumes both are sorted by cluster and minute start
        # Assumes base_file has all minute_starts without gaps
        # Assumes join_file is a subset of base_file
        if int(join_lines[pointer].split()[1])/60000*60 == minute_start:
            pointer = pointer + 1
            tmp = join_lines[pointer].split()
            whole_line = base_split + tmp[2:]
        else:
            whole_line = base_split + ['NULL']*9

        out_file.write(whole_line)



if __name__=='__main__':
    main()
