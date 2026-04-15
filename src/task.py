
"""
ANARISK Backend API
Direction Générale des Impôts - Burkina Faso
Système d'Analyse des Risques Fiscaux
"""

import os
import sys
import logging
from datetime import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
from dotenv import load_dotenv
from celery.utils.log import get_task_logger
import redis

# Ajouter le dossier src au path
sys.path.append(os.path.dirname(__file__))

from core.data_loader import DataLoader
from core.risk_compute import RiskComputer
from core.fiches_generator import FichesGenerator
from utils.util import get_latest_risk_file
from db.ods import connectionOds
from celery import Celery
from flask_sqlalchemy import SQLAlchemy
# Import du package API
from api import register_blueprints 
from extensions import db as f_db
from core import sql_ifu
from sqlalchemy import create_engine
from data_extractor import DataExtractor
from flask_apscheduler import APScheduler
from config import Config
import uuid
from task_logger import task_context
from task_manager import task_manager
load_dotenv()

logger = get_task_logger(__name__)
database_url = os.getenv("DATABASE_URL")
DATA_DIR ="../data"
OUTPUT_DIR ="../output"
PROGRAMME_DIR ="../programmes"
FICHES_DIR ="../fiches"
DOCS_DIR ="../docs"
RISK_CONTRIBUABLE_DIR = "../data/risk_contribuables"

IFU_DB_URL = os.getenv("IFU_DB_URL")
ODS_DB_URL = os.getenv("ODS_DB_URL")
# Init variables pour Celery
oracle_engine = connectionOds()

def _generate_fiches_core(quantume, task_logger=None):
    """
    Logique métier de génération des fiches (inchangée).
    lire risk_data dans le dossier data/risk_contribuables/ pour le quantume donné,
    extract_ifu,extract_dgd
    """
    log = task_logger if task_logger else logger

    try:
        if task_logger:
            task_logger.info(f"Initialisation de la génération des fiches pour le quantum '{quantume}'")
            task_logger.set_progress(5, "Initialisation des composants")
        
        loader = DataLoader(oracle_engine=oracle_engine)
        extractor = DataExtractor()

        if task_logger:
            task_logger.set_progress(10, "Chargement des données IFU et DGD")
        
        engin = create_engine(IFU_DB_URL).connect()
        data_fu = extractor.extract_data(engin, sql_ifu)
        data_dgb =loader.extract_dgd(oracle_engine.connect())
        
        data_ifu = pd.read_csv("../data/data_ifu.csv")
        data_dgd =  pd.read_csv("../data/data_dgb.csv")
        
        if task_logger:
            task_logger.info(f"Données IFU chargées : {len(data_ifu)} lignes")
            task_logger.info(f"Données DGD chargées : {len(data_dgd)} lignes")
            task_logger.set_progress(30, "Chargement des fichiers du quantum")

        #files paths
        risk_data_path = f"../data/risk_contribuables/{quantume}.csv"
        quantume_path  = f"../programmes/{quantume}.xlsx"
        
        #data
        quantume_df = pd.read_excel(quantume_path, skiprows=1)
        risk_data = pd.read_csv(risk_data_path, sep=";")

        if task_logger:
            task_logger.info(f"Programme du quantum chargé : {len(quantume_df)} lignes")
            task_logger.info(f"Données de risque chargées : {len(risk_data)} lignes")
            task_logger.set_progress(50, "Génération des fiches en cours...")

        generator = FichesGenerator(quantume_name=quantume,
                                    quantume=quantume_df,
                                    ifu_data=data_ifu,
                                    risk_data=risk_data,
                                    data_dgd=data_dgd)
        generator.generate()
        
        if task_logger:
            task_logger.set_progress(100, "Génération des fiches terminée")
            task_logger.success(f"Fiches générées avec succès pour le quantum '{quantume}'")
        
    except Exception as e:
        if task_logger:
            task_logger.error(f"Erreur lors de la génération des fiches : {str(e)}")
        raise
    finally:
        pass


def generate_fiches(quantume, task_id=None):
    """
    Wrapper avec logging pour la génération des fiches.
    Si task_id est fourni, utilise le système de logging du TaskManager.
    Sinon, exécute directement la fonction core.
    """
    if task_id:
        # Utiliser le système de logging avec task_context
        with task_context(task_id, "Génération des fiches", quantume=quantume) as task_logger:
            _generate_fiches_core(quantume, task_logger)
            return {'status': 'success', 'quantume': quantume}
    else:
        # Exécution directe sans logging avancé (rétrocompatibilité)
        _generate_fiches_core(quantume)
        return {'status': 'success', 'quantume': quantume}


def _run_risk_analysis_core(quantume, task_logger=None):
    """
    Logique métier de l'analyse des risques (inchangée).
    Extraction, fusion et calcul des indicateurs de risque.
    Étapes :
    1. Connexion Oracle
    2. Extraction & fusion des données (DataLoader)
    3. Calcul des indicateurs de risque (RiskComputer)
    4. Sauvegarde CSV dans data/risk_contribuables/
    """
    log = task_logger if task_logger else logger
    connect = None
    
    try:
        if task_logger:
            task_logger.info(f"Démarrage de l'analyse des risques pour le quantum '{quantume}'")
            task_logger.set_progress(5, "Connexion à la base de données Oracle")
        
        # ── Étape 1 : Connexion Oracle ────────────────────────────────────
        connect = oracle_engine.connect()
        
        if task_logger:
            task_logger.info("Connexion Oracle établie avec succès")
            task_logger.set_progress(10, "Extraction et fusion des données")

        # ── Étape 2 : Extraction & fusion ────────────────────────────────
        loader = DataLoader(oracle_engine=oracle_engine)
        merged_data = loader.run_extract_merge()

        if merged_data is None or merged_data.empty:
            raise ValueError('Aucune donnée retournée par DataLoader.run_extract_merge()')

        log.info(f"Données fusionnées : {len(merged_data)} lignes, {len(merged_data.columns)} colonnes")
        
        if task_logger:
            task_logger.set_progress(50, "Calcul des indicateurs de risque")

        # ── Étape 3 : Calcul des indicateurs ─────────────────────────────
        computer = RiskComputer()
        result = computer.run(data=merged_data, quantume_name=quantume)

        if result.get('status') != 'success':
            raise RuntimeError(result.get('message', 'Erreur inconnue dans RiskComputer.run()'))

        summary = {
            'nb_contribuables': result.get('nb_contribuables', 0),
            'nb_indicateurs':   result.get('nb_indicateurs', 0),
            'elapsed_time':     result.get('elapsed_time', 0),
            'file':             result.get('file', ''),
            'generated_at':     datetime.now().isoformat(),
        }
        
        log.info(
            f"Analyse terminée — {summary['nb_contribuables']} contribuables, "
            f"{summary['nb_indicateurs']} indicateurs, "
            f"{summary['elapsed_time']}s"
        )
        
        if task_logger:
            task_logger.set_progress(100, "Analyse des risques terminée")
            task_logger.success(
                f"Pré-liste générée : {summary['nb_contribuables']} contribuables, "
                f"{summary['nb_indicateurs']} indicateurs"
            )
        
        return {'status': 'done', **summary}

    except Exception as exc:
        log.exception(f"Échec de run_risk_analysis : {exc}")
        if task_logger:
            task_logger.error(f"Échec de l'analyse : {str(exc)}")
        raise

    finally:
        # Libérer la connexion Oracle
        if connect is not None:
            try:
                connect.close()
                if task_logger:
                    task_logger.info("Connexion Oracle fermée")
            except Exception:
                pass


def run_risk_analysis(quantume, task_id=None):
    """
    Wrapper avec logging pour l'analyse des risques.
    Si task_id est fourni, utilise le système de logging du TaskManager.
    Sinon, exécute directement la fonction core.
    """
    if task_id:
        # Utiliser le système de logging avec task_context
        with task_context(task_id, "Analyse des risques", quantume=quantume) as task_logger:
            result = _run_risk_analysis_core(quantume, task_logger)
            # Stocker le résultat dans la tâche
            task = task_manager.get_task(task_id)
            if task:
                task.complete(result)
            return result
    else:
        # Exécution directe sans logging avancé (rétrocompatibilité)
        return _run_risk_analysis_core(quantume)

