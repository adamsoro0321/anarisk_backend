from data_loader import DataLoader
from risk_compute import RiskComputer
import os
import sys
from core.data_loader import DataLoader
from db.ods import connectionOds

oracle_engine = connectionOds()
connect = oracle_engine.connect()
loader = DataLoader(oracle_engine)

#1.=============extraction des données
merged_data = loader.run_extract_merge()

print(f"Data loaded and merged successfully with shape: {merged_data.shape}")