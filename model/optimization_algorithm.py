import random


class OptimizationAlgorithmm(object):

    def __init__(self, utility_function, metrics, services, constraints=[] ,eval_limit=100):
        self.ufunction = utility_function
        self.objectives = metrics
        self.service_list = services
        self.eval_limit = eval_limit
        self.constraints = constraints
        self.evals = 0




class HillClimber(OptimizationAlgorithmm):

    def execute(self, mutation_strength=1):
        self.evals = 0

        intial_soltution = self.initlilize_solution()

        best = self.evaluate_solution(intial_soltution)
        not_changed = 0
        while self.evals <= self.eval_limit:
            mutation = self.mutate_solution(best[0], mutation_strength)
            mutation_val = self.evaluate_solution(mutation)
            if mutation_val[2] <= best[2]:
                best = mutation_val
            else:
                not_changed +=1

            # If we do not improve in a tenth of all evals, break
            if not_changed > self.eval_limit / 10:
                break
        return best



    def initlilize_solution(self):

        solution_vector = [random.randint(0,1) for i in range(len(self.service_list))]
        while not self.check_feasibility(solution_vector):
            solution_vector = [random.randint(0, 1) for i in range(len(self.service_list))]
        return solution_vector


    def evaluate_solution(self, solution_vector):
        solution_set = self.encode_solution(solution_vector)
        objective_vector =  self.objectives.metrics(solution_set)
        utility = self.ufunction.eval(objective_vector)
        self.evals += 1
        return (solution_set, objective_vector, utility)

    def encode_solution(self, solution_vector):
        solution_set = []
        for elem, source in zip(solution_vector, self.service_list):
            if elem == 1:
                solution_set.append(source)
        return solution_set

    def decode_solution(self, solution_set):
        solution_vector = []
        for service in self.service_list:
            if service in solution_set:
                solution_vector.append(1)
            else:
                solution_vector.append(0)

        return solution_vector


    def check_feasibility(self, solution_vector):
        if sum(solution_vector) == 0:
            return False


        for constraint in self.constraints:
            if constraint.feasible(solution_vector) == False:
                return False

        return True

    def mutate_solution(self, solution_set, mutation_strength):

        solution_vector = self.decode_solution(solution_set)

        def mutate(solution_vector):
            position = random.randint(0,len(solution_vector)-1)
            if solution_vector[position] == 1:
                solution_vector[position] = 0
            else:
                solution_vector[position] = 1
            return solution_vector

        for i in range(mutation_strength):
            solution_vector = mutate(solution_vector)
            while not self.check_feasibility(solution_vector):
                solution_vector = mutate(solution_vector)

        return solution_vector
