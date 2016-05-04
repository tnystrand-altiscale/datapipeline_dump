import sys

def main():
    base_file = open(sys.argv[1])
    join_file = open(sys.argv[2])
    out_file = open('merged.tsv','w')

    base_head = base_file.readline()
    join_head = join_file.readline()
   
    complete_header = "\t".join( base_head.split() + join_head.split()[3:] )
    print(complete_header)
    out_file.write(complete_header + "\n")

    base_lines = base_file.readlines()
    join_lines = join_file.readlines()
    pointer = 0

    for base in base_lines:
        
        base_split = base.split()
        minute_start = int(base_split[0])
       
        # Assumes both are sorted by cluster and minute start
        # Assumes base_file has all minute_starts without gaps
        # Assumes join_file is a subset of base_file
        tmp = join_lines[pointer].split()
        if int(tmp[1])/60000*60 == minute_start and tmp[0] == base_split[10]:
            pointer = pointer + 1
            while int(tmp[1])/60000*60 == minute_start:
                pointer = pointer+1
                tmp = join_lines[pointer].split()

            whole_line = base_split + tmp[3:]
            print whole_line
        else:
            whole_line = base_split + ['NULL']*9

        whole_line = "\t".join(whole_line)

        out_file.write(whole_line + "\n")



if __name__=='__main__':
    main()
