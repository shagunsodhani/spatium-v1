from miner import Miner

a = Miner(create_table = 1, minPrevalance = 0.001, threshold_distance=1000)
a.initialise()
a.colocation_2()
a.colocation_k(4)
