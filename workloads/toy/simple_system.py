from simulator.event_queue import EventQueue
from simulator.resource import *
from simulator.dag import Dag
from simulator.system import System
from workloads.toy.linear_dag import linear_dag_clockwork_data, linear_instance_list, linear_instance_placements

class SimpleSystem(System):
	pools: Dict[str, ResourcePool]

	def __init__(self,_events: EventQueue, _pools: Dict[str, ResourcePool]):
		super().__init__(_events)
		self.pools = _pools
		self.dag_maps = {}

	def schedule(self, curr_time, events, *args, **kwargs):
		# First check for any completed functions
		for name, pool in self.pools.items():
			for resource in pool.get_all_resources():
				completed = resource.remove_at_time(curr_time)
				for (fid, tag) in completed:
					assert tag in self.outstanding_requests, "Tag needs to map to an outstanding request"
					self.outstanding_requests[tag] = (True, self.outstanding_requests[tag][1])
		# Now process any new events
		for (dag, input) in events:
			# for linear_instance in linear_instance_list:
			# 	print(linear_instance.id_res_map)
			# 	print(linear_instance.running_time)
			# 	print(linear_instance.running_cost)
			# for price_instance in linear_instance_placements.price_list:
			# 	print(price_instance.running_cost)
			# for time_instance in linear_instance_placements.time_list:
			# 	print(time_instance.running_time)
			# sample_placement = (linear_instance_placements.get_sample_list(10000, 10000))[0]
			# self.dag_maps = sample_placement.id_res_map
			print(linear_dag_clockwork_data)
			if linear_dag_clockwork_data[1][0] < 20 and linear_dag_clockwork_data[1][1] < 85:
				self.dag_maps[dag.name] = 'STD_GPU'
			elif linear_dag_clockwork_data[0][0] < 20 and linear_dag_clockwork_data[0][1] < 85:
				self.dag_maps[dag.name] = 'STD_CPU'
			else:
				continue
			# print(dag_maps)
			# for sample_instance in linear_instance_placements.get_sample_list(10000, 10000):
			# 	print(sample_instance.running_time)
			# 	print(sample_instance.running_cost)
			# print("Done")
			# print("Hello")
			dag.execute()  # Need to do this to seal the DAG
			self.outstanding_requests[self.__generate_tag(dag, curr_time)] = (True, dag)
		# Now schedule functions
		for tag, (flag, dag) in self.outstanding_requests.copy().items():
			if flag:
				if dag.has_next_function():
					# Find which resource is faster
					nxt = dag.peek_next_function()
					# std_cpu = nxt.resources['STD_CPU']
					# std_gpu = nxt.resources['STD_GPU']
					# cpu_time = std_cpu['pre'].get_runtime() + std_cpu['exec'].get_runtime() + std_cpu['post'].get_runtime()
					# gpu_time = std_gpu['pre'].get_runtime() + std_gpu['exec'].get_runtime() + std_gpu['post'].get_runtime()
					# if cpu_time < gpu_time:
					# 	pool = self.pools['STD_CPU_POOL']
					# else:
					# 	pool = self.pools['STD_GPU_POOL']
					# print(self.dag_maps)
					# print(nxt.unique_id)
					if self.dag_maps[dag.name] == 'STD_GPU':
						pool = self.pools['STD_GPU_POOL']
						# print("GPU")
					else:
						pool = self.pools['STD_CPU_POOL']
						# print("CPU")
					# If there is a resource available, schedule it
					result : Optional[Tuple[str, Resource]] = pool.find_first_available_resource(nxt, tag)
					if result:
						(name, rsrc) = result
						rsrc.add_function(dag.next_function(), tag, curr_time)
						self.outstanding_requests[tag] = (False, self.outstanding_requests[tag][1])
				else:
					# Remove if there is no next function
					self.outstanding_requests.pop(tag)

	def __generate_tag(self, dag: Dag, time: int):
		return f"{dag.name}:{time}:{id(dag)}"

	def __decode_tag(self, tag: str) -> Dag:
		return self.outstanding_requests[tag]