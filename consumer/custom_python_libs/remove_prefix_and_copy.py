from __future__ import print_function
import sys, re, os
import shutil, time, argparse
import subprocess
import json

def copy_from_remote(data_filepath, data_filename, json_path):
        with open(json_path,'r') as json_file:
            json_settings = json.load(json_file)
        
        scpstr = json_settings['scp_str']
       
    	copyfromdogfood = 'scp ' + scpstr + '/' + data_filename + ' ' + data_filepath + '/' + data_filename

        print('\n\t Command: %s' % copyfromdogfood,end='')
        sys.stdout.flush()

        subprocess.check_call(copyfromdogfood.split())
   

def remove_prefix(data_filepath):

    fd = open(data_filepath,'rw')
    header = fd.readline()
    
    if "." in header:
        split_header = re.split('\t|\n', header)
        # Pick out last item of ever 'dot' split
        column_names = [head.split('.')[-1] for head in split_header]
        column_names.append("\n")
    	
        new_header = "\t".join(column_names)
    	
        new_filepath = ".".join([data_filepath, "~temp~", str(int(time.time())), 'tmp', 'tsv'])
        fd_new = open(new_filepath ,'w')
        fd_new.writelines(new_header)
    	
        shutil.copyfileobj(fd,fd_new)
    	
        fd_new.close()
        fd.close()
        shutil.move(new_filepath,data_filepath)
    else:
        fd.close()

def parser_arguments():
    parser = argparse.ArgumentParser(description='Copy from dogfood and remove table prefix')
    
    parser.add_argument('-f', '--filename', required=True, nargs=1, \
        help='Name of file to be copied')

    parser.add_argument('-c', '--copy', default=[1], type=int, choices=[0, 1], nargs=1, \
        help='If set to 1 copy')

    parser.add_argument('-r', '--remove_prefix', default=[1], type=int, choices=[0, 1], nargs=1, \
        help='If set to 1 remove_refix')

    parser.add_argument('-j', '--json', nargs=1, \
        help='File path of json running script = directory where file will be saved')

    parser.add_argument('-d', '--directory', required=True, nargs=1, \
        help='Name of directory where file is copied from')

    return parser.parse_args()

def main():
    args = parser_arguments()
    if args.json:
        json_location = os.path.realpath(args.directory[0] + '/' +  args.json[0])
    else:
        json_location = os.path.realpath(args.directory[0] + '/' +  'run_etl_procedure.json')
        
    # Output goes where json file is
    outfile_path = os.path.dirname(json_location)
    outfile_name = args.filename[0]

    outfile = outfile_path + '/' + outfile_name

    if args.copy[0]:
        print('Copying to %s...' % outfile, end='')
        copy_from_remote(outfile_path, outfile_name, json_location)
        print('[DONE]')
    
    if args.remove_prefix[0]:
        print('Removing in %s ...' % outfile, end='')
        remove_prefix(outfile)
        print('[DONE]')

if __name__ == '__main__':
    main()
