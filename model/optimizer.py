from itertools import chain, combinations
import datetime as dt
import math
from .optimization_algorithm import HillClimber
import logging
logging.basicConfig(level=logging.INFO)

def powerset(iterable, min_size=1):
    "list(powerset([1,2,3])) --> [(), (1,), (2,), (3,), (1,2), (1,3), (2,3), (1,2,3)]"
    s = list(iterable)
    return chain.from_iterable(combinations(s, r) for r in range(min_size, len(s)+1))



class Optimizer(object):

    def __init__(self, metrics, utility_function, naive=True, eval_limit=1000):
        self.metric = metrics
        self.utility_function = utility_function
        self.naive = naive
        self.eval_limit = eval_limit


    def optimize(self, service_set, pattern=None):


        algorithm = HillClimber(self.utility_function, self.metric, service_set, eval_limit=self.eval_limit)
        heuristic = False
        # Run Naive if configured or the possible set of solutions is smaller than the evaluation limit
        if self.naive or math.pow(2, len(service_set)) <= self.eval_limit :
            t0 = dt.datetime.now()
            optimized_result = self.naive_optimization(service_set, pattern)
        else:
            logging.info("Hill Climber")
            heuristic = True
            t0 = dt.datetime.now()
            optimized_result = algorithm.execute()
        delta = (dt.datetime.now() - t0).total_seconds()
        return optimized_result, delta, heuristic


    def non_optimized(self, service_set, pattern=None):
        objective_values = self.metric.metrics(service_set, pattern)
        utility = self.utility_function.eval(objective_values)
        return (service_set, objective_values, utility)

    def naive_optimization(self, service_set, pattern=None):

        results = []
        for partial_set in powerset(service_set):
            objective_values = self.metric.metrics(partial_set, pattern)
            utility = self.utility_function.eval(objective_values)
            results.append([partial_set, objective_values, utility])

        # Return option which minizes the utility
        sorted_results = sorted(results, key=lambda x: x[2], reverse=False)
        return sorted_results[0]

