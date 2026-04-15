from data_loader import DataLoader
from risk_compute import RiskComputer
import os
import sys
from core.data_loader import DataLoader
from db.ods import connectionOds
import logging


oracle_engine = connectionOds()
connect = oracle_engine.connect()
loader = DataLoader(oracle_engine)
computer = RiskComputer()

#1.=============extraction des données
merged_data = loader.run_extract_merge()

#2.==========compute
r = computer.run(data=merged_data,quantume_name="Q1_2026",indicateurs=[1])
print(f"Data loaded and merged successfully with shape: {merged_data.shape}")