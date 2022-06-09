from utils import to_dict, DecompositionOptimizer
from model import Optimizer, Weighted_Sum, Generic_Metric
import json
from pprint import pprint
from optparse import OptionParser

def decomposition_to_query(decomposition, optimize=False, qm_file=""):

    decomposition_dict = to_dict(decomposition)

    with open("decompositions/service_map.json", 'r') as rep_file:
        # The Service map is needed to map the names in the decomposition to
        # the URL of the SPARQL Endpoints
        # This needs to be adjusted for new federations
        service_map = json.load(rep_file)

    ## Set the Utility Function
    w = None
    uf = Weighted_Sum(weights=w)

    ## Get Metrics
    m = Generic_Metric(qm_file)
    optimizer = Optimizer(m, uf, naive=False, eval_limit=1000)

    ## Get
    decomposition_tool = DecompositionOptimizer(optimizer, service_map, optimize)

    query = decomposition_tool.to_sparql(decomposition_dict)
    return query

def get_options():
    parser = OptionParser()
    parser.add_option("-d", "--decompositionfile", dest="decomp_file", type="string",
                      help="File with the SPARQL Query Decompositon generated by ARQ")
    parser.add_option("-o", "--optimize", action="store_true", default=False,
                      help="Optimize the Alternative Service Expressions in the query")

    parser.add_option("-q", "--quality_metric_fn", dest="qm_file",
                      help="File with the quality metrics for the SPARQL endpoints")

    parser.add_option("-v", "--verbose", action="store_true", default=False,
                      help="Verbose option outputs the query and also meta data about the query.")

    (options, args) = parser.parse_args()
    return options, args


if __name__ == '__main__':

    (options, args) = get_options()
    query = decomposition_to_query(options.decomp_file, options.optimize,options.qm_file)
    if not options.verbose:
        pprint(str(query['query']))
    else:
        pprint(query)


    ## Usage python3 prepare_query.py -d decompositions/type1/CD1_decomp.txt -q quality_descriptions/1_qm/description_qm_1_v_1.json -o
    ## Usage python3 prepare_query.py -d decompositions/type1/CD1_decomp.txt -q quality_descriptions/1_qm/description_qm_1_v_1.json