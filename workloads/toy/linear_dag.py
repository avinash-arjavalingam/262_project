from simulator.dag import Dag, Function
from simulator.resource import ResourceType
from simulator.runtime import ConstantTime
from .constants import *
from random import randint, sample
from bisect import bisect

# linear_first = Function(
# 	unique_id='linear_first',
# 	resources= {
# 		'STD_CPU' : {
# 			'type' : ResourceType.CPU,
# 			'space': 100.0,  # Ignoring space this function requires on the CPU
# 			'pre'  : ConstantTime(1),
# 			'exec' : ConstantTime(3),
# 			'post' : ConstantTime(0)
# 		},
# 		'STD_GPU' : {
# 			'type' : ResourceType.GPU,
# 			'space': 100.0,
# 			'pre'  : ConstantTime(1),
# 			'exec' : ConstantTime(2),
# 			'post' : ConstantTime(0)
# 		}
# 	}
# )
#
# linear_second = Function(    # This function takes a long time to run on a CPU
# 	unique_id='linear_second',
# 	resources= {
# 		'STD_CPU' : {
# 			'type' : ResourceType.CPU,
# 			'space': 100.0,  # Ignoring space this function requires on the CPU
# 			'pre'  : ConstantTime(1),
# 			'exec' : ConstantTime(5),
# 			'post' : ConstantTime(0)
# 		},
# 		'STD_GPU' : {
# 			'type' : ResourceType.GPU,
# 			'space': 100.0,
# 			'pre'  : ConstantTime(1),
# 			'exec' : ConstantTime(1),
# 			'post' : ConstantTime(0)
# 		}
# 	}
# )
#
# linear_third = Function(     # This function takes a long time to run on a GPU
# 	unique_id='linear_third',
# 	resources= {
# 		'STD_CPU' : {
# 			'type' : ResourceType.CPU,
# 			'space': 100.0,  # Ignoring space this function requires on the CPU
# 			'pre'  : ConstantTime(1),
# 			'exec' : ConstantTime(1),
# 			'post' : ConstantTime(0)
# 		},
# 		'STD_GPU' : {
# 			'type' : ResourceType.GPU,
# 			'space': 100.0,
# 			'pre'  : ConstantTime(1),
# 			'exec' : ConstantTime(5),
# 			'post' : ConstantTime(0)
# 		}
# 	}
# )

linear_first = Function(
	unique_id='linear_first',
	resources= {
		'STD_CPU' : {
			'type' : ResourceType.CPU,
			'space': 100.0,  # Ignoring space this function requires on the CPU
			'pre'  : ConstantTime(1),
			'exec' : ConstantTime(3),
			'post' : ConstantTime(0)
		},
		'STD_GPU' : {
			'type' : ResourceType.GPU,
			'space': 100.0,
			'pre'  : ConstantTime(1),
			'exec' : ConstantTime(1),
			'post' : ConstantTime(0)
		}
	}
)

linear_second = Function(    # This function takes a long time to run on a CPU
	unique_id='linear_second',
	resources= {
		'STD_CPU' : {
			'type' : ResourceType.CPU,
			'space': 100.0,  # Ignoring space this function requires on the CPU
			'pre'  : ConstantTime(1),
			'exec' : ConstantTime(5),
			'post' : ConstantTime(0)
		},
		'STD_GPU' : {
			'type' : ResourceType.GPU,
			'space': 100.0,
			'pre'  : ConstantTime(1),
			'exec' : ConstantTime(2),
			'post' : ConstantTime(0)
		}
	}
)

linear_third = Function(     # This function takes a long time to run on a GPU
	unique_id='linear_third',
	resources= {
		'STD_CPU' : {
			'type' : ResourceType.CPU,
			'space': 100.0,  # Ignoring space this function requires on the CPU
			'pre'  : ConstantTime(1),
			'exec' : ConstantTime(8),
			'post' : ConstantTime(0)
		},
		'STD_GPU' : {
			'type' : ResourceType.GPU,
			'space': 100.0,
			'pre'  : ConstantTime(1),
			'exec' : ConstantTime(3),
			'post' : ConstantTime(0)
		}
	}
)

# Add costs to functions
all_funs = [linear_first, linear_second, linear_third]
for f in all_funs:
	for rsrc_name, specs in f.resources.items():
		if rsrc_name == 'STD_CPU':
			specs['cost'] = COST_PER_CPU_TIME * specs['exec'].get_runtime()
		else:
			specs['cost'] = COST_PER_GPU_TIME * specs['exec'].get_runtime()


linear_dag = Dag('linear', funs=[linear_first, linear_second, linear_third])
linear_dag.add_edge(linear_first, linear_second)
linear_dag.add_edge(linear_second, linear_third)
linear_dag.sanity_check()


def gen_clockwork(dag_functions):
	dag_cpu_time = 0
	dag_cpu_cost = 0
	dag_gpu_time = 0
	dag_gpu_cost = 0
	for func in list(dag_functions):
		dag_cpu = func.resources['STD_CPU']
		dag_gpu = func.resources['STD_GPU']
		dag_cpu_time += dag_cpu['pre'].get_runtime() + dag_cpu['exec'].get_runtime() + dag_cpu['post'].get_runtime()
		dag_gpu_time += dag_gpu['pre'].get_runtime() + dag_gpu['exec'].get_runtime() + dag_gpu['post'].get_runtime()
		dag_cpu_cost += dag_cpu['cost']
		dag_gpu_cost += dag_gpu['cost']
	return [[dag_cpu_time, dag_cpu_cost], [dag_gpu_time, dag_gpu_cost]]

linear_dag_clockwork_data = gen_clockwork(linear_dag.functions.values())


class DAGInstance:

	def __init__(self, dag):
		self.dag = dag
		self.running_time = 0
		self.running_cost = 0
		# self.functions_per_resource = {}
		self.id_res_map = {}
		# self.id_max_map = {}

		# for res in ["GPU", "CPU"]:
		# 	self.functions_per_resource[res] = []

	# def add_func_res(self, function, resource):
	# 	func_tuple = (function.id, function.get_max_memory(resource))
	# 	self.functions_per_resource[resource].append(func_tuple)

	def copy_dag_instance(self):
		new_dag_instance = DAGInstance(self.dag)
		for id_one, res in list(self.id_res_map.items()):
			new_dag_instance.id_res_map[id_one] = res
		# for id_two, max_prev in self.id_max_map:
		# 	new_dag_instance.id_max_map[id_two] = max_prev
		# for i in range(len(self.functions_per_resource)):
		# 	for func_tuple in self.functions_per_resource[i]:
		# 		new_tuple = (func_tuple[0], func_tuple[1])
		# 		new_dag_instance.functions_per_resource[i].append(new_tuple)
		new_dag_instance.running_cost = self.running_cost
		new_dag_instance.running_time = self.running_time
		return new_dag_instance

	def update_dag_instance(self, this_function, res):
		self.id_res_map[this_function.unique_id] = res
		# func_time = func.get_resource_runtime(res) + self.id_max_map[func.id]
		# for root_next_func in func.next_funcs:
		# 	next_max_time = 0
		# 	if root_next_func.id in self.id_max_map:
		# 		next_max_time = self.id_max_map[root_next_func.id]
		# 	self.id_max_map[root_next_func.id] = max(func_time, next_max_time)
		# self.running_time = max(self.running_time, func_time)
		func_res = this_function.resources[res]
		self.running_time = self.running_time + func_res['pre'].get_runtime() + func_res['exec'].get_runtime() + func_res['post'].get_runtime()
		self.running_cost = self.running_cost + func_res['cost']
		# self.add_func_res(func, res)
		# self.id_max_map.pop(func.id, None)

resource_list = ['STD_CPU', 'STD_GPU']

def gen_dag_instances(dag):
	dep_queue = dag
	instance_list = []

	root = dep_queue.pop(0)
	for root_res in list(resource_list):
		root_dag_instance = DAGInstance(dag)
		root_dag_instance.id_res_map[root.unique_id] = root_res
		# print(root_dag_instance.id_res_map[root.unique_id])
		# for root_next_func in root.next_funcs:
		# 	root_dag_instance.id_max_map[root_next_func.id] = root.get_resource_runtime(root_res)
		root_func_res = root.resources[root_res]
		root_dag_instance.running_time = root_func_res['pre'].get_runtime() + root_func_res['exec'].get_runtime() + root_func_res['post'].get_runtime()
		root_dag_instance.running_cost = root_func_res['cost']
		# root_dag_instance.add_func_res(root, root_res)
		instance_list.append(root_dag_instance)

	while len(dep_queue) > 0:
		function = dep_queue.pop(0)
		new_instance_list = []
		for dag_instance in instance_list:
			for res in list(resource_list):
				new_dag_instance = dag_instance.copy_dag_instance()
				new_dag_instance.update_dag_instance(function, res)
				new_instance_list.append(new_dag_instance)
		instance_list = new_instance_list

	# for finished_dag_instance in instance_list:
	# 	for func_res in list(finished_dag_instance.functions_per_resource.values()):
	# 		sorted(func_res, key=lambda x: x[1])

	return instance_list


def select_pareto_instances(instance_list):
	pareto_list = []

	for instance in instance_list:
		pareto_add = True
		for comp_instance in instance_list:
			if not (instance is comp_instance):
				if (comp_instance.running_time <= instance.running_time) and (comp_instance.running_cost <= instance.running_cost):
					pareto_add = False
					break
		if pareto_add:
			pareto_list.append(instance)

	return pareto_list

linear_instance_list = select_pareto_instances(gen_dag_instances([linear_first, linear_second, linear_third]))

class DAGSelector:

	def __init__(self, instance_list, sample_size):
		self.price_list = sorted(instance_list, key=lambda x: x.running_cost)
		self.time_list = sorted(instance_list, key=lambda x: x.running_time)
		self.sample_size = int(max(min(sample_size, len(self.price_list)), 1))

	def binary_find_index(self, value, this_list, type):
		keys = []
		if type == "price":
			keys = [this_inst.running_cost for this_inst in this_list]
		else:
			keys = [this_inst.running_time for this_inst in this_list]
		pos = (bisect(keys, value, 0, len(this_list)))
		return pos

	def get_sample_list(self, price_slo, time_slo):
		sample_list = []
		price_index = self.binary_find_index(price_slo, self.price_list, "price")
		time_index = self.binary_find_index(time_slo, self.time_list, "cost")
		if (price_index <= 0) or (time_index <= 0):
			return []

		end_index = len(self.price_list) - time_index
		valid_size = price_index - end_index
		if valid_size <= 0:
			return []

		valid_list = self.price_list[end_index:price_index]
		min_size = min(self.sample_size, len(valid_list))
		sample_list = sample(valid_list, min_size)
		return sample_list

	def get_placements(self, cluster, sample_instance):
		function_place_map = {}
		for res, res_list in list(sample_instance.functions_per_resource.items()):
			res_nodes = cluster.nodes_by_res[res]
			updated_nodes = []
			for func_id, func_mem in res_list:
				placed = False
				done = False
				while (not placed) and (not done):
					if len(res_nodes) == 0:
						done = True
					elif func_mem <= res_nodes[0].memory:
						function_place_map[func_id] = res_nodes[0].id
						res_nodes[0].memory = res_nodes[0].memory - func_mem
						placed = True
					else:
						popped_node = res_nodes.pop(0)
						updated_nodes.append(popped_node)
				if done:
					break
			if len(res_nodes) == 0:
				cluster.nodes_by_res[res] = sorted(updated_nodes, key=lambda x: x.memory)
				return {}
			else:
				res_nodes.extend(updated_nodes)
				cluster.nodes_by_res[res] = sorted(res_nodes, key=lambda x: x.memory)

		return function_place_map

linear_instance_placements = DAGSelector(linear_instance_list, 1)