import sys, os, shutil
import logging
import time, datetime
SELFPATH = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.realpath(SELFPATH + '/../../../datapipeline/common_python_scripts/'))
import argparse, logging_related
from run_sanity_queries import Launcher

# Runs a query in tez or mapreduce # number of times in different queues
# Example usage:
#   python launch_probe_queries.py
#       --logfile cron_out_1.log
#       --username pipeline


DEFAULT_PSTR = ' (default: %(default)s)'
# Put namespace flags from parser to dictionary and check that it has the right format and numbers
def parse_input_args():
    settings = {}
    args = parser_arguments()
    settings['engine'] = args.engine[0]
    settings['loglevel'] = args.loglevel[0]
    settings['logfile'] = args.logfile[0]
    settings['username'] = args.username[0]
    settings['queue'] = args.queue[0]
    settings['date'] = args.date[0]
    settings['datadir'] = args.datadir[0]

    # Defaulting to today-2days since current pipeline is 2 days behind
    if settings['date'] is None:
        settings['date'] = str(datetime.date.today()-datetime.timedelta(2))

    if settings['datadir'] is None:
        dir = SELFPATH + '/' + 'all_data' + '/' + settings['date']
        if os.path.exists(dir):
            shutil.rmtree(dir)
        os.mkdir(dir)
        settings['datadir'] = dir
    return settings

def parser_arguments():
    parser = argparse.ArgumentParser(description='Launching hive query with mr, tez or spark')

    parser.add_argument('-e', '--engine', default=['tez'], nargs=1, \
        help='Execution engine ')

    parser.add_argument('-l', '--loglevel', default=['info'], nargs=1, \
        help='Log level' + DEFAULT_PSTR)

    parser.add_argument('-o', '--logfile', default=[SELFPATH + '/log.out'], nargs=1, \
        help='Log file' + DEFAULT_PSTR)

    parser.add_argument('-u', '--username', default=['tnystrand'], nargs=1, \
        help='Default Hadoop username, used to identify queries' + DEFAULT_PSTR)

    parser.add_argument('-q', '--queue', default=['production'], nargs=1, \
        help='Default queue to launch query in' + DEFAULT_PSTR)

    parser.add_argument('-d', '--date', default=[None], nargs=1, \
        help='Date to run queries for' + DEFAULT_PSTR)

    parser.add_argument('-m', '--datadir', default=[None], nargs=1, \
        help='Directory to save output files in' + DEFAULT_PSTR)

    return parser.parse_args()


def main(): 
    # Set hadoop user name to distinguish from pipeline jobs
    # Parse input arguments and set a logging level
    settings = parse_input_args()
    os.environ['HADOOP_USER_NAME'] = settings['username']
    logger = logging_related.get_logger( \
        logging_related.LEVELS.get(settings['loglevel']), 
        settings['logfile'])

    # Run all jobs sequentially
    logger.info(60*'-')
    structTime = time.localtime()
    logger.info('Starting sanity checks at %s' % str(datetime.datetime(*structTime[:6])))
    logger.info(60*'-')
    logger.info('Settings used:\n %s' % str(settings))
    launcher = Launcher(logger, settings['engine'], settings['queue'], settings['date'], settings['datadir'])

    path = SELFPATH + '/sanity_queries'
    launcher.run_all_sequential(path)
    logger.info('Finished successfully')

    # Clean up hadoop user name
    del os.environ['HADOOP_USER_NAME']

if __name__ == '__main__':
    main()
