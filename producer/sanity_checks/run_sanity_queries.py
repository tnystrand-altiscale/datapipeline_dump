import time, sys, os, re
import subprocess
selfpath = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, selfpath + '/../../common_python_scripts/')
import logging_related

class Launcher:
    """ Contains methods related to obtain data for bursting performance
    The methods launch a set of jobs whose performance can be reviewd in yarn logs
    """

    def __init__(self, logger, engine, queue, date, outputfolder):
        self.logger = logger
        self.outputfolder = outputfolder
        self.date_str = date
        # Set common variables for all launchers
        queues = ['default', 'interactive', 'production', 'research']

        if not queue in queues:
            self.logger.error('\nBad queue: "' + queue  +  '"\nExiting with angry errors.')
            sys.exit(1)

        if engine == 'mr':
            hive_settings = ['set hive.execution.engine=mr',
                             'set mapreduce.map.memory.mb=8120',
                             'set mapreduce.map.java.opts=-Xmx7608m',
                             'set mapreduce.reduce.memory.mb=8120',
                             'set mapreduce.reduce.java.opts=-Xmx7608m']
            queue_str = 'set mapred.job.queue.name='+queue
            executor = 'hive'
            flag = '-e'
        elif engine == 'tez':
            hive_settings = ['set hive.execution.engine=tez',
                             'set hive.tez.java.opts=-Xmx7096m',
                             'set hive.tez.container.size=8000',
                             'set tez.cpu.vcores=4',
                             'set tez.session.client.timeout.secs=10000000',
                             'set tez.session.am.dag.submit.timeout.secs=10000000']
            queue_str = 'set tez.queue.name='+queue
            executor = 'hive'
            flag = '-e'
        else:
            self.logger.error('\nEngine "' + engine + '" not recognized. Exiting with angry errors.')
            sys.exit(1)

        self.logger.debug('Selected settings:\n%s' % (hive_settings))
        self.logger.debug('Selected queue: ' + queue_str)
        self.logger.debug('Selected executor: ' + executor)

        hive_settings.append(queue_str)
        self.query_prefix = ''.join([settings + ';\n' for settings in hive_settings]) 
        self.executor = executor
        self.flag = flag 
        self.logger.debug('Full settings: ' + self.query_prefix)


    def launch_query(self, query):
        """ Run a query with a certain engine and queue """
        query_text = ''
        with open(query) as f:
            query_text = ''.join(f.readlines())
            # Hive does not support -e flag with hivevar settings
            # Hive has a bug when specifing -f and a commandline queue if hiverc queue also is set
            # Therefore we must set queue with -e flag and parse ${hivevar:date} in code. lame.
            query_text = query_text.replace('${hivevar:date}',self.date_str)


        self.logger.info('Constructing commands for queries')
        query_full = self.query_prefix + query_text
        exe_command = ' '.join([self.executor, self.flag, '"' + query_full + '"'])

        # Loop over all defined commands sequentially 
        self.logger.info('Running command:\n' + exe_command)
        time0 = time.time()
        p = subprocess.Popen(exe_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        data, err = p.communicate()
        self.logger.info('Query running time: %dmin' % int((time.time() - time0)/60))

        logname = os.path.split(query)[1] + '.log'
        logfilepath = self.outputfolder + '/' + logname
        self.logger.info('Writing to logfile: %s' % logfilepath)
        with open(logfilepath,'w') as f:
            if not data is None:
                f.write(data)
            else:
                self.logger.error('\nQuery problem:\n%s', err)
                f.write(err)



    def run_all_sequential(self, path):
        """ Run all queries found at path """
        self.logger.info('Looking for files at path: ' + path) 
        files = os.listdir(path)
        query_files = []
        for file in files:
            m = re.search('.sql$',file)
            if not m is None:
                filepath = path + '/' + file
                query_files.append(filepath)
                self.logger.debug('Found file: ' + filepath)

        for query in query_files:
            self.launch_query(query)

        

