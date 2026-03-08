
import os
import sys
from core.data_loader import DataLoader
from core.risk_compute import RiskComputer
from db.ods import connectionOds

oracle_engine = connectionOds()
connect = oracle_engine.connect()
loader = DataLoader(oracle_engine)
computer = RiskComputer()

#1.=============extraction des données
merged_data = loader.run_extract_merge()


#2.=============calcul des indicateurs de risque
computer.calculate_all_indicators(merged_data=merged_data)
r = computer.run(data=merged_data)

#3.=======================


print(f"Data loaded and merged successfully with shape: {merged_data.shape}")