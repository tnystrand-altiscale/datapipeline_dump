from __future__ import print_function
import sys, re, os
import shutil, time, argparse
import runcommand

"""
0. jobid               	string
1. containerid         	string
2. requestedtime       	bigint
3. reqreservedtime        	bigint
4. allocatedtime       	bigint
5. acquiredtime        	bigint
6. expiredtime         	bigint
7. runningtime         	bigint
8. killedtime          	bigint
9. releasedtime        	bigint
10. completedtime       	bigint
11. memory              	bigint
12. vcores              	int
13. queue               	string
14. host                	string
15. priority            	int
16. account             	bigint
17. cluster_uuid        	string
18. principal_uuid      	string
19. user_key            	string
20. clustermemory       	bigint
21. numberapps          	bigint
22. cluster_vcores          bigint
23. system              	string
24. date                	date
"""

def getrow(idstart,idend,name,split_line):
    return "\t".join([split_line[0],
			      	split_line[1], 
			      	split_line[idstart], 
		      		split_line[idend],
		      		str(int(split_line[idend])-int(split_line[idstart])),
		      		name,
                    split_line[11],
                    split_line[15],
                    split_line[14],
		      		split_line[23],
		      		split_line[24],
                    '\n'])
    
# data_filename should be filepath
def split_data(data_filename):
    try:
        fd = open(data_filename,'rw')
    except IOError as e:
        print("I/O error({0}): {1}".format(e.errno, e.strerror))
    except:
        print("Unexpected error:", sys.exc_info()[0])
        raise

    old_header = fd.readline() # Headerline
    split_header = re.split('\t+|\\s+',old_header)

    header = "\t".join([split_header[0], 
                        split_header[1], 
                        "start", 
                        "stop", 
                        "duration", 
                        "event", 
                        "size", 
                        "priority", 
                        "hostname",
                        split_header[23], 
                        split_header[24], 
                        "\n"])
    
    new_filename = ".".join([data_filename,"flat","tsv"])
    fd_new = open(new_filename ,'w')
    fd_new.writelines(header)
    
    for line in fd:
        split_line = re.split('\t+|\\s+',line)

        # REQUEST
        if int(split_line[2])>0 and int(split_line[4])>0:
        	fd_new.writelines(getrow(2,4,"request",split_line))
         
        # ALLCOATE
        if int(split_line[4])>0 and int(split_line[7])>0:
        	fd_new.writelines(getrow(4,7,"allocate",split_line))
        
        # RUNCOMPLETE
        if int(split_line[7])>0 and int(split_line[10])>0:
        	fd_new.writelines(getrow(7,10,"run",split_line))
        
        # ALLOCATED TO RELEASED
        if int(split_line[4])>0 and int(split_line[9])>0:
        	fd_new.writelines(getrow(4,9,"released",split_line))
        
        # RESERVED TO ALLOCATED
        if int(split_line[3])>0 and int(split_line[4])>0:
        	fd_new.writelines(getrow(3,4,"reserved",split_line))

        # RESERVED TO KILLED
        if int(split_line[3])>0 and int(split_line[8])>0:
        	fd_new.writelines(getrow(3,8,"reserved",split_line))
    
        # reserved to completed no run
        if int(split_line[3])>0 and int(split_line[10]>0 and int(split_line[2])==0 and int(split_line[4])==0)>0:
        	fd_new.writelines(getrow(3,10,"reserved",split_line))
        
        # KILLED
        if int(split_line[7])>0 and int(split_line[8])>0:
        	fd_new.writelines(getrow(7,8,"killed",split_line))
    
    fd.close()
    fd_new.close()


def parser_arguments():
    parser = argparse.ArgumentParser(description='Split container_fact into separate events')
    
    parser.add_argument('-f', '--filename', required=True, nargs=1, \
        help='Name of container_fact data to be separated')

    parser.add_argument('-d', '--directory', required=True, nargs=1, \
        help='Name of directory where file is copied from')

    return parser.parse_args()


def main():
    args = parser_arguments()
    split_data(args.directory[0] + '/' + args.filename[0])


if __name__=="__main__":
    main()

