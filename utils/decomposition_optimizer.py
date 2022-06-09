import sys
sys.path.insert(0, '../')
import re
import logging
logging.basicConfig(level=logging.WARNING)

class DecompositionOptimizer(object):

    def __init__(self, optimizer, service_map, optimize=False):
        self.__service_map = service_map
        self.optimize = optimize
        if optimizer != None:
            self.optimizer = optimizer
            self.query_utility = 0
            self.optimization_total_seconds = 0
            self.optimized_subqueries = 0
        else:
            self.optimizer = None

    def to_sparql(self, decomp_dict):
        self.query_utility = 0
        self.optimization_total_seconds = 0
        self.optimized_subqueries = 0
        self.service_count_max = 0
        self.heuristic_applied = False
        for elem in decomp_dict:
            if "ProjectionElemList" in elem.keys():
                head = self.process_projection(elem)

            else:
                body = self.process_elem(elem)

        overall_utility = self.query_utility
        if self.optimized_subqueries == 0:
            avg_utility = 0
        else:
            avg_utility = (self.query_utility / self.optimized_subqueries)
        logging.info("Sum utility: {0}; Mean utility: {1}".format(overall_utility, avg_utility))
        if self.optimize:
            logging.info("Optimization overhead: {0}".format(self.optimization_total_seconds))
            if self.optimization_total_seconds > 1:
                logging.warning("Optimization overhead: {0}".format(self.optimization_total_seconds))

        eval_limit = 0
        if self.optimizer:
            if not self.optimizer.naive :
                eval_limit = self.optimizer.eval_limit

        logging.info("Service Count Maximum {0}".format(self.service_count_max))
        return {
            "query" : "{0} {{\n  {1}\n}}".format(head,body),
            "utility_sum" : str(overall_utility),
            "utility_mean" : str(avg_utility),
            "optimization_ms" : str(self.optimization_total_seconds),
            "utility_function" : str(self.optimizer.utility_function.name),
            "utility_function_weights" : str(self.optimizer.utility_function.weights),
            "description_file": self.optimizer.metric.sd_file,
            "quality_metric_count" : self.optimizer.metric.number_of_quality_metrics,
            "service_group_count" : self.optimized_subqueries,
            "eval_limit" : eval_limit,
            "service_count_max" : self.service_count_max,
            "heuristic_applied" : self.heuristic_applied
        }

    def process_elem(self, elem, key=None):
        if type(elem) == list:
            if key == "Union" or key == "NUnion":
                res = []
                for item in elem:
                    res.append(self.process_elem(item))
                return "UNION".join(["{{ \n{0} }}\n".format(r) for r in res if len(r) >1])
            elif key == "NJoin" or key == "EmptyNJoin":
                res = []
                for item in elem:
                    res.append(self.process_elem(item))
                return " ".join(res)
            elif key == "LeftJoin":
                res = []
                for item in elem:
                    res.append(self.process_elem(item))
                return " OPTIONAL ".join(["{{ \n{0} }}\n".format(r) for r in res])
            elif "ExclusiveGroup":
                res = []
                rgx = r"\{(.*?)\}"
                for item in elem:
                    res.append(self.process_elem(item))

                service = res[0].split("{")[0]
                new_res = []
                for r in res:
                    new_res.append(re.findall(rgx, r)[0])

                group_str = "{0} {{\n {1} }}\n".format(service," \n".join(new_res))
                return group_str


        elif type(elem) == dict:
            if "ExclusiveStatement" in elem.keys():
                return self.exclusive_statement(elem['ExclusiveStatement'])
            elif "StatementSourcePattern" in elem.keys():
                # Select the correspponding optimization method
                return self.statement_source_pattern_optimized(elem['StatementSourcePattern'])
                #return self.statement_source_pattern_optimized_replicas(elem['StatementSourcePattern'])
            else:
                key = list(elem.keys())[0]
                return self.process_elem(elem[key], key)
        else:
            return ""

    def exclusive_statement(self,statement):
        # statement = statement.replace(")", ")\n")
        pattern = self.get_triple_pattern(statement)
        rgx = r"id=(.*),"
        service = re.findall(rgx, statement)[0].split("_")
        service_url = self.__service_map[service[-2]][service[-1]]['url']
        return "SERVICE <{0}> {{ {1} }}\n".format(service_url, pattern)

    def statement_source_pattern(self, statement):
        # statement = statement.replace(")", ")\n")
        pattern = self.get_triple_pattern(statement)
        rgx = r"id=(.*?),"
        services = re.findall(rgx, statement)
        elems = []
        for service in services:
            service_s = service.split("_")
            service_url = self.__service_map[service_s[-2]][service_s[-1]]['url']
            elems.append("{{ SERVICE <{0}> {{ {1} }} }}\n".format(service_url, pattern))
        return "{" + " UNION ".join(elems) + "}"

    def statement_source_pattern_optimized_replicas(self, statement):
        pattern = self.get_triple_pattern(statement)
        rgx = r"id=(.*?),"
        services = re.findall(rgx, statement)
        elems = []
        replication_dict = {}
        for service in services:
            dataset = service.split("_")[2]
            replication_dict.setdefault(dataset,[]).append(service)

        optimized = []
        for replications in replication_dict.values():
            if self.optimize:
                result, delta, heuristic_applied = self.optimizer.optimize(replications, pattern)
                if heuristic_applied: self.heuristic_applied = True
                self.optimization_total_seconds += delta
                if len(result[0]) > 1: logging.info("{0} services selected, for pattern '{1}'".format(len(result[0]), pattern))
            else:
                result = self.optimizer.non_optimized(replications, pattern)
            optimized.extend(result[0])
            self.query_utility += result[2]
            self.optimized_subqueries += 1


        for service in optimized:
            service_s = service.split("_")
            service_url = self.__service_map[service_s[-2]][service_s[-1]]['url']
            elems.append("{{ SERVICE <{0}> {{ {1} }} }}\n".format(service_url, pattern))
        return "{" + " UNION ".join(elems) + "}"


    def statement_source_pattern_optimized(self, statement):
        pattern = self.get_triple_pattern(statement)
        rgx = r"id=(.*?),"
        services = re.findall(rgx, statement)
        elems = []

        if len(services) > self.service_count_max:
            self.service_count_max = len(services)

        optimized = []
        if self.optimize:
            result, delta, heuristic_applied = self.optimizer.optimize(services, pattern)
            if heuristic_applied: self.heuristic_applied = True
            self.optimization_total_seconds += delta
            if len(result[0]) > 1: logging.info("{0} services selected, for pattern '{1}'".format(len(result[0]), pattern))
        else:
            if not self.optimizer is None:
                result = self.optimizer.non_optimized(services, pattern)
            else:
                result = ([0], 0, 0)
        optimized.extend(result[0])
        self.query_utility += result[2]
        self.optimized_subqueries += 1


        for service in optimized:
            service_s = service.split("_")
            service_url = self.__service_map[service_s[-2]][service_s[-1]]['url']
            elems.append("{{ SERVICE <{0}> {{ {1} }} }}\n".format(service_url, pattern))
        return "{" + " UNION ".join(elems) + "}"


    def get_triple_pattern(self, statement):
        rgx = r"Var\s\((.*?)\)"
        vars = re.findall(rgx, statement)
        pattern = ""
        for var in vars:
            var = var.split(",")
            if len(var) > 1:
                if "http://" in var[1]:
                    pattern += "<{0}> ".format(var[1][7:])
                else:
                    pattern += '"{0}"'.format(var[1][8:-1])
            else:
                pattern += "?" + var[0][5:] + " "
        pattern += "."
        return pattern

    def process_projection(self, projection_dict):
        return "SELECT {0} WHERE ".format(" ".join(["?" + var for var in projection_dict['ProjectionElemList']]))

