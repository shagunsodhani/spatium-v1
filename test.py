from miner import Miner

a = Miner(create_table = 1, minPrevalance = 0.1, threshold_distance=10000)
a.initialise()
a.colocation_2()
a.colocation_k(3)
