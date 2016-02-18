from __future__ import print_function
import sys, hashlib, re


def main():
    datafile = sys.argv[1] # Name of input file to be processed
    keyword = sys.argv[2] # Name of keyword in head (typically table name)
    id_col = int(sys.argv[3]) # Index of column to be encrypted 
    outfile = datafile + '_' + 'anonymous' + '.tsv'
    fd_new = open(outfile,'wb')


    with open(datafile,'rb') as hiveout:
        print("Reading till header...",end="")
        # Read till header
        tmpline = hiveout.readline()
        while tmpline.find(keyword)==-1:
            tmpline = hiveout.readline()
        print("[DONE]")

        print("Removing database name in header...",end="")
        new_header = tmpline
        # Remove header prefix
        if "." in new_header:
             split_header = re.split('\t|\n', new_header)
             # Pick out last item of ever 'dot' split
             column_names = [head.split('.')[-1] for head in split_header]
             column_names.append("\n")
        
             new_header = "\t".join(column_names)
        print("[DONE]")

        print("Processing file...",end="")
        fd_new.write(new_header)
        for chunk in read_in_chunks(hiveout):
            for line in chunk:
                list_line = line.split('\t')
                list_line[id_col] = hashlib.sha1(list_line[id_col]).hexdigest()
                fd_new.write("\t".join(list_line))
        print("[DONE]")

        fd_new.close()


def read_in_chunks(file_object, chunk_size=16384):
    """Lazy function (generator) to read a file piece by piece.
    Default chunk size: 16k."""
    while True:
        data = file_object.readlines(chunk_size)
        if not data:
            break
        yield data

if __name__== '__main__':
    main()
