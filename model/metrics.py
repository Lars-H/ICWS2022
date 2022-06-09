"""
Compute the Quality metrics and aggregation
according to the provided source descriptions

"""

import json
import numpy as np

class Metric(object):

    def __init__(self, source_description_fn):
        with open(source_description_fn, 'r') as sd_file:
            self.descriptions = json.load(sd_file)
            self.sources = self.descriptions['sources']
            self.metric_aggregation = self.descriptions['metrics']
            self.sd_file = source_description_fn

    def metrics(self, source_selction, pattern=None):
        pass

    @property
    def number_of_quality_metrics(self):
        return len(self.metric_aggregation)


    def aggregation_function(self, name):
        """
        Returns aggregation function by its name
        :param name: Name of the aggregation function
        :return: Aggregated function
        """
        return {
            "min" : np.min,
            "max" : np.max,
            "mean": np.mean,
            "sum" : np.sum
        }[name]


class Generic_Metric(Metric):

    def metrics(self, source_selction, pattern=None):

        objective_values = []
        for metric, agg_func_name in zip(self.metric_aggregation.keys(), self.metric_aggregation.values()):
            f = self.aggregation_function(agg_func_name)
            try:

                objective_values.append(f([self.sources[source][metric] for source in source_selction]))
            except Exception as e:
                print(e)
        return objective_values

