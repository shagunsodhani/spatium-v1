from miner import Miner

a = Miner(create_table = 1, minPrevalance = 0.2, threshold_distance=5000)
# a.initialise()
# a.colocation_2()
# a.colocation_k(3)
a.colocation_k(4)