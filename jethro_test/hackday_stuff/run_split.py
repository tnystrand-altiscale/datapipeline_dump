import sys, os

def main():
    """
    Intended usage:

    python run_etl_procedure.py --filename data.tsv
    
    Where
    -- data.tsv                     container_fact file to be split
    """
    currentpath = os.path.dirname(os.path.realpath(__file__))
   
    command = ' '.join(['python', '/home/tnystrand/semi_serious/consumer/custom_python_libs/split_containerfact.py'] +
                    sys.argv[1:] +
                    ['--directory', currentpath])
    os.system(command)
    
if __name__ == '__main__':
    main()
