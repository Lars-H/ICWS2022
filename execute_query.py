from optparse import OptionParser
from utils import run_arq
import os
import logging
logging.basicConfig(level=logging.INFO)

def get_options():

    parser = OptionParser()
    parser.add_option("-q", "--queryfile", dest="queryfile", type="string",
                      help="File with the SPARQL Query", metavar="QUERY")
    parser.add_option("-a", "--arq", dest="arq_cli", type="string",
                      help="ARQ CLI Binary to execute the query")

    parser.add_option("-o", "--outputdir", dest="outputdir", type="string", default=os.getcwd() + "/",
                      help="Output Directory (Optional)")

    parser.add_option("-t", "--timeout", dest="timeout",  type="int", default=500,
                      help="Set the maximium execution time for ARQ in seconds (Optional)")

    (options, args) = parser.parse_args()
    return options, args

if __name__ == '__main__':

    (options, args) = get_options()
    query_file = os.path.abspath(options.queryfile)
    stats = run_arq(query_file, options.outputdir, options.arq_cli, options.timeout)

    ## Usage:
    ## python3 execute_query.py -q queries/examples/example_query.rq -a <add_jena_directory>/bin/arq