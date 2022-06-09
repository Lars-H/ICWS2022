"""
Functions to execute the ARQ CLI for queries
and record metadata of the query and execution time as well

"""
from optparse import OptionParser
import os
import datetime as dt
import subprocess
import pandas as pd
import io
import json
import logging
import sys
from subprocess import STDOUT, check_output

logger = logging.getLogger("ARQ util")
logger.setLevel(logging.INFO)

def run_arq_file(query_file,arq_dir, timeout=0):
    """
    Execute ARQ Binary with a query file
    :param query_file: SPARQL Query File
    :param arq_dir: ARQ Binary
    :param timeout: Maximum Execution time
    :return: Output of ARQ CLI, Measured Timedelta and Service Count in Query
    """
    head, tail = os.path.split(query_file)

    cmd = "{0} --query={1} --results=CSV --time".format(arq_dir, query_file)
    # Execute Arq
    t0 = dt.datetime.now()
    logger.info("Starting at: {0}".format(t0))
    try:
        t0 = dt.datetime.now()
        if timeout > 0:
            std_out = std_err = check_output(cmd, stderr=STDOUT, timeout=timeout, shell=True)
        else:
            std_out = std_err = check_output(cmd, stderr=STDOUT, shell=True)
        delta = dt.datetime.now() - t0
    except Exception as e:
        logger.info("Timeout of {0} seconds expired.".format(timeout))
        logger.exception(e)
        std_out = std_err = None
        delta = dt.timedelta(seconds=timeout)

    services = open(query_file).read().count("SERVICE")
    return std_out, std_err, delta, services


def run_arq(query_file, out_dir, arq_dir, timeout=0):
    """
    Run ARQ with a query file and record statistic
    :param query_file: SPARQL Query File
    :param out_dir: Directory for the results and statistics
    :param arq_dir: ARQ Binary
    :param timeout: Maximium execution time
    :return: statistics of the exection
    """


    head, tail = os.path.split(query_file)
    results_file = tail.split(".")[0] + "_results.csv"


    stats = {
        "queryname": tail.split(".")[0],
        "query_file": query_file
    }
    std_out, std_err, delta, services = run_arq_file(query_file,arq_dir,timeout)

    logger.info("Execution time: {0}".format(delta))
    try:

        if std_out == None and std_err == None:
            arq_time = timeout
            n_results = 0
            unique_results = 0
            interrupted = True
        else:
            std_err = std_err.decode('UTF-8').split("\nTime:")[1]
            std_out = std_out.decode('UTF-8').split("\nTime:")[0]
            interrupted = False
            arq_time = float(str(std_err).split(" ")[1].replace(",","."))
            results = pd.read_csv(io.StringIO(std_out))

            unique_results = len(results.drop_duplicates())
            n_results = len(results)
            results.to_csv(out_dir + results_file)
            logger.info("Results written to: {0}".format(out_dir + results_file))

        stats.update({
            "timestamp" : str(dt.datetime.now()),
            "elapsed_s" : delta.total_seconds(),
            "arq_execution_time": arq_time,
            "results" : n_results,
            "unique_results": unique_results,
            "service_count" : services,
            "timeout" : str(timeout),
            "interrupted": str(interrupted),
        })

        meta_file = out_dir + "{0}_stats.json".format(tail)
        meta_to_file(meta_file, stats)
        logger.info("Statistics written to: {0}".format(meta_file))
    except Exception as e:
        logger.exception(std_out, std_err)
        raise e
    return stats


def meta_to_file(meta_file, stats):

    new_stats = [stats]
    if os.path.isfile(meta_file):
        with open(meta_file, 'r') as meta:
            old_stats = json.load(meta)
            new_stats.extend(old_stats)
    with open(meta_file, 'w') as out_file:
        json.dump(new_stats, out_file)

