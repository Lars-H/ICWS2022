"""
Utility Functions
- Weighted Sum

"""

import numpy as np

class UtilityFunction(object):

    def eval(self, x):
        """
        Evaluates the utility of a solution
        :param x: Aggregation function values
        :return: Utility value
        """
        pass

    @property
    def name(self):
        pass

    @property
    def weights(self):
        pass

class Weighted_Sum(UtilityFunction):
    """
    Implements a weighted sum utility function
    Weights are passed on initialization
    """


    def __init__(self, **kwargs):
        ## Paramters: Weights for each aggrgation function
        self.__weights = kwargs.get("weights", None)

    def eval(self, x):
        # Weighted Sum without weights = Mean
        if self.__weights is None:
            return np.mean(x)

        # Compute the weighted sum
        return sum([w_i * x_i for w_i, x_i in zip(self.__weights, x)])

    @property
    def name(self):
        return "Weighted sum"

    @property
    def weights(self):
        return self.__weights