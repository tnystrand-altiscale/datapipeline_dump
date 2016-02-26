import sys, os

def main():
    """
    Intended usage:

    python run_etl_procedure.py --filename data.tsv -- json run_etl_procedure.json
    
    Where
    -- data.tsv                     is the name of file on cluster side
    -- remove_prefix_and_copy.json  is json file with connection settings
    """
    currentpath = os.path.dirname(os.path.realpath(__file__))
   
    command = ' '.join(['python', '/Users/admin/altiscale/custom_python_libs/remove_prefix_and_copy.py'] +
                    sys.argv[1:] +
                    ['--directory', currentpath])
    os.system(command)
    
if __name__ == '__main__':
    main()
