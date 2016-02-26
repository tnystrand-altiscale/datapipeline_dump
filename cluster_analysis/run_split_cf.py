import sys
sys.path.insert(0,'/Users/admin/altiscale/custom_python_libs')
import split_containerfact as scf

scf.split_data(os.path.realpath(sys.argv[1]))

