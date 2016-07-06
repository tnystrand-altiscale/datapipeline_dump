#!/usr/bin/python

from __future__ import print_function
import os
import sys
import logging
import time
import re
import subprocess

from common_python_scripts import argparse

RUN_MODES = {'append' : 'append_missing', 'purge_last' : 'purge_last_partition', 'purge_all' : 'purge_all'}
HDFS_STAGING = '/data/log_pipeline/jethro_staging/'

log = logging.getLogger(__name__)

def main():
    settings = parse_input_args()
    log.debug(settings)

    hiveconfs = {}
    start_date = settings['start_date']
    end_date = settings['end_date']
    hdfs_dir = settings['data_dir']

    # Try to read queries
    raw_projection = read_all_lines(settings['project_query_file'])
    raw_description = read_all_lines(settings['description_file']) 

    #log.info("\n{0:-^30}\n{1}\n{2:-^30}\n{3}".format("Project Query", ''.join(raw_projection), "Description File", ''.join(raw_description)))

    # Pad with or remove overwrite from input description file
    fixed_desc = insert_overwrite_command_fix(raw_description, settings['overwrite'])
    log.info("\n{0:-^30}\n{1}".format("Description file", fixed_desc))

    fixed_desc_filename = settings['description_file'] + '.tmp'
    with open(fixed_desc_filename, 'w') as fixed_desc_file:
        fixed_desc_file.write(fixed_desc)

    # Purge all
    if settings['mode'] == RUN_MODES['purge_all']:
        if not start_date:
            start_date = "1970-01-01"
        if not end_date:
            end_date = time.strftime('%Y-%m-%d', time.gmtime())
        hdfs_dir = hdfs_dir + 'purge_all'


    log.info('Start date: {0}'.format(start_date))
    log.info('End date: {0}'.format(end_date))

    hiveconfs['start_date'] = start_date
    hiveconfs['end_date'] = end_date
    hiveconfs['tgt_dir'] = hdfs_dir

    if settings['mode'] == RUN_MODES['purge_all']:
        create_hdfs_dir(hdfs_dir)
        clear_hdfs_location(hdfs_dir)
        project_query = build_project_query(hiveconfs, hdfs_dir, raw_projection)
        log.info("\n{0:-^30}\n{1}".format("Parsed Query", project_query))
        project_to_hdfs(project_query)
        # Consider loading file by file
        load_to_jethro(hdfs_dir, fixed_desc_filename)
        clear_hdfs_location(hdfs_dir)


def create_hdfs_dir(hdfs_dir):
    full_command = 'hdfs dfs -mkdir -p "{0}/"'.format(hdfs_dir)
    log.debug("\n{0:-^30}\n{1}".format("HDFS command", full_command))
    #run_bash_command(full_command)

def clear_hdfs_location(hdfs_dir):
    full_command = 'hdfs dfs -rm -r "{0}/*"'.format(hdfs_dir)
    log.debug("\n{0:-^30}\n{1}".format("HDFS command", full_command))
    #run_bash_command(full_command, ignore_err=True)


def load_to_jethro(hdfs_dir, fixed_desc_filename):
    full_command = 'JethroLoader pipeline {0} {1}'.format(fixed_desc_filename, hdfs_dir)
    log.debug("\n{0:-^30}\n{1}".format("Jethro command", full_command))
    #run_bash_command(full_command)

def project_to_hdfs(project_query):
    full_command = 'hive -e "{0}"'.format(project_query)
    log.debug("\n{0:-^30}\n{1}".format("Hive command", full_command))
    #run_bash_command(full_command, ignore_err=True)

def build_project_query(hiveconfs, hdfs_dir, projection):
    parsedquery = ''
    prog = re.compile('\$\{hiveconf\:[^}]*\}', flags=re.IGNORECASE)
    for row in projection:
        confs = prog.findall(row)
        for conf in confs:
            configname = conf[11:-1].lower()
            try:
                row = row.replace(conf, hiveconfs[configname])
            except KeyError:
                log.error('hiveconf {0!r} not found in settings {1!r}'.format(configname, hiveconfs))
                sys.exit(1)
        parsedquery = parsedquery + row
    return parsedquery

def insert_overwrite_command_fix(raw_description, overwrite):
    # Expect to find overwrite after TABLE tablename
    # Split into words
    fixed_desc = ''.join([w.lower() for w in raw_description])
    words = fixed_desc.split()
    try:
        index = words.index('table')
    except ValueError:
        log.error('{0!r} not found in desc files\n:'.format('table', ''.join(raw_description)))
        sys.exit(1) 

    overwrite_exists = words[index + 2].lower() == 'overwrite'

    # Add overwrite
    if overwrite and not overwrite_exists:
        tablename = words[index + 1]
        fixed_desc = fixed_desc.replace(tablename, tablename + ' \noverwrite \n', 1) 

    # Remove overwrite
    if not overwrite and overwrite_exists:
        fixed_desc = fixed_desc.replace('overwrite', '', maxreplace=1) 

    return fixed_desc


def run_bash_command(cmd, ignore_err=False, exit_on_error=True):
     p = subprocess.Popen(cmd,stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
     (out, err) = p.communicate()
     if not ignore_err:
         if exit_on_error and err!="":
             log.error('Bash command\n{0!r}\nfailed with\n{1}'.format(cmd, err))
             sys.exit(1)
         return (out,err)
     return out

# Query must contain a proper table name
def extract_table_name(project_query_file):
    query_lines = read_all_lines(project_query_file)

    # Finding if query has a from clause
    index = [index for index, value in enumerate(query_lines) if "from " in value or "from\n" in value] 

    if len(index) == 0:
        raise ValueError("<from> must be present in query:\n {0!s}".format(query_lines))
    if len(index) > 1:
        raise ValueError("Found multiple 'from' in query:\n {0!s}".format(query_lines))
        
    # Check if table name are on same or next lines
    # Select end of query and remove empty lines
    end_of_query = ' '.join([ line.strip(' \n\t') for line in query_lines[ index[0]+1 : ] ])
    lines = end_of_query.split()

    if not lines or not lines[0]:
        raise ValueError("Could not find a table-name in the query")

    # Select database.table name
    return lines[0]

# Fuction for returning all lines
def read_all_lines(filename):
    lines = None
    with open(filename) as f:
        lines = f.readlines()
    return lines

def parse_input_args():
    settings = {}
    args = parser_arguments()
    settings['description_file'] = args.description_file[0]
    settings['project_query_file'] = args.project_query_file[0]
    settings['date_name'] = args.name[0]
    settings['mode'] = args.mode[0]

    settings['start_date'] = args.start_date[0]
    settings['end_date'] = args.end_date[0]
    settings['data_dir'] = args.data_dir[0]

    settings['overwrite'] = True

    # Parsing
    settings['table'] = extract_table_name(settings['project_query_file'])
    log.info("Found table {0!r}".format(settings['table']))

    if not settings['data_dir']:
        settings['data_dir'] = HDFS_STAGING + settings['table'].replace('.', '/') + '/'

    if settings['mode'] == RUN_MODES['purge_all']:
        settings['overwrite'] = True

    return settings

def parser_arguments():
    parser = argparse.ArgumentParser(description='Extract from table to hdfs and load to Jethro')

    parser.add_argument('-c', '--description_file', required=True, nargs=1, \
            help='Description file which accepts overwrite command')

    parser.add_argument('-p', '--project_query_file', required=True, nargs=1, \
            help='Project query which accepts data range')

    parser.add_argument('-n', '--name', required=True, nargs=1, \
            help='Name of date column. E.g. <date>, <partition_date>')

    parser.add_argument('-m', '--mode', required=True, nargs=1, choices=RUN_MODES.values(), \
            help='Mode of jethro load. <append_missing>, <purge_last_partition>, <purge_all>')

    parser.add_argument('-s', '--start_date', default = [None], nargs=1, \
            help='Start date of appending/purging - optional. Purges from date or fills in missing from date.')

    parser.add_argument('-e', '--end_date', default = [None], nargs=1, \
            help='End date of appending/purging - optional. Purges to date or fills in missing to date.')

    parser.add_argument('-d', '--data_dir', default = [None], nargs=1, \
            help='HDFS dir name - defaults to table name')
    return parser.parse_args()

if __name__ == '__main__':
    time0 = time.time()
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.DEBUG, filename='lastrun.log', filemode='w')
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(levelname)s:%(message)s')
    ch.setFormatter(formatter)
    log.addHandler(ch)
    main()
    log.info('Runtime: %ds' % int(time.time() - time0))
