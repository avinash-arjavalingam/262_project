from simulator.event_queue import EventQueue
from simulator.resource import *

from workloads.toy.linear_dag import linear_dag
from workloads.toy.branch_dag import branch_dag
from workloads.toy.simple_system import SimpleSystem



cpu_pool = ResourcePool("STD_CPU_POOL", ResourceType.CPU, [("STD_CPU_1", Resource("STD_CPU", ResourceType.CPU))])
gpu_pool = ResourcePool("STD_GPU_POOL", ResourceType.GPU, [("STD_GPU_1", Resource("STD_GPU", ResourceType.GPU))])

# Add DAGs here
events = EventQueue([
	(linear_dag, [(0, None), (0, None), (0, None), (0, None), (1, None), (1, None)])
	# (branch_dag, [(0, None), (0, None), (0, None), (0, None), (1, None), (1, None)])
])

system = SimpleSystem(events,
					  {
						  "STD_CPU_POOL" : cpu_pool,
						  "STD_GPU_POOL" : gpu_pool
					  })


if __name__ == "__main__":
	overall_latency = system.run()
	print(f"Time all functions finished is: {overall_latency}")

