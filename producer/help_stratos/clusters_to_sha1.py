import sys, hashlib

def main():
    datafile = sys.argv[1] # Name of input file to be processed
    outfile = datafile + '.' + 'anonymous' + '.txt'

    fd_out = open(outfile, 'w')
    with open(datafile, 'r') as fd_in:
        for line in fd_in.readlines():
            line_stripped = line.rstrip('\n')
            fd_out.write(line_stripped + '\t' + hashlib.sha1(line_stripped).hexdigest() + '\n')
        fd_out.close()

if __name__ == '__main__':
    main()
