# services/contribuable_service.py
from globals import connectionOds
import logging
import pandas as pd
import numpy as np
class ContribuableService:
    def __init__(self):
        pass
    
    def _get_connection(self):
        """Établit la connexion à la base Oracle"""
        try:
            connection = connectionOds()

            return connection
        except Exception as e:
            logging.error(f"Erreur de connexion Oracle: {str(e)}")
            raise

    def get_douane_data(self, ifu):
        """Récupère les données douanières pour un IFU donné depuis la base Oracle
        Args:
            ifu: Numéro IFU du contribuable
        Returns:
            dict: Dictionnaire contenant {found, data, summary, count}
        """
        connection = None
        try:
            query = """
                SELECT * FROM sid_dgd_CPF
	            WHERE EXTRACT(YEAR FROM DATE_LIQUIDATION) in (2022,2023,2024)
   	                AND IFU = :ifu
            """
            connection = self._get_connection().connect()
            df = pd.read_sql_query(query, connection, params={'ifu': ifu})
            
            if df.empty:
                return {
                    'found': False,
                    'data': [],
                    'summary': {},
                    'count': 0
                }
            df = df.replace({pd.NA: None, pd.NaT: None})
            df = df.where(pd.notnull(df), None)
            df = df.replace({np.nan: None})

            # Convertir en dictionnaire
            data = df.to_dict('records')
            
            # TODO: Calculer les statistiques selon les colonnes réelles
            summary = {
                'total_records': len(df),
                'annees': df['ANNEE'].tolist() if 'ANNEE' in df.columns else []
            }

            return {
                'found': True,
                'data': data,
                'summary': summary,
                'count': len(data)
            }

        except Exception as e:
            logging.error(f"Erreur lors de la récupération des données douanières: {str(e)}")
            raise
        finally:
            if connection:
                connection.close()
                
    def get_insd_data(self, ifu):
        """Récupère les données INSD pour un IFU donné depuis la base Oracle
        Args:
            ifu: Numéro IFU du contribuable
        Returns:
            dict: Dictionnaire contenant {found, data, summary, count}
        """
        connection = None
        try:
            # TODO: Adapter la requête selon le schéma exact de la base
            query = """
                SELECT * FROM TABLE_INSD 
                WHERE IFU = :ifu
                ORDER BY ANNEE DESC
            """
            connection = self._get_connection().connect()
            df = pd.read_sql_query(query, connection, params={'ifu': ifu})
            
            if df.empty:
                return {
                    'found': False,
                    'data': [],
                    'summary': {},
                    'count': 0
                }
            
            # Convertir en dictionnaire
            data = df.to_dict('records')
            
            # TODO: Calculer les statistiques selon les colonnes réelles
            summary = {
                'total_records': len(df),
                'annees': df['ANNEE'].tolist() if 'ANNEE' in df.columns else []
            }
            
            return {
                'found': True,
                'data': data,
                'summary': summary,
                'count': len(data)
            }
            
        except Exception as e:
            logging.error(f"Erreur lors de la récupération des données INSD: {str(e)}")
            raise
        finally:
            if connection:
                connection.close()
                
    def get_sonabel_data(self, ifu):
        """Récupère les données SONABEL pour un IFU donné depuis la base Oracle
        Args:
            ifu: Numéro IFU du contribuable
        Returns:
            dict: Dictionnaire contenant {found, data, summary, count}
        """
        connection = None
        try:
            # TODO: Adapter la requête selon le schéma exact de la base
            query = """
                SELECT * FROM TABLE_SONABEL 
                WHERE IFU = :ifu
                ORDER BY ANNEE DESC
            """
            connection = self._get_connection().connect()
            df = pd.read_sql_query(query, connection, params={'ifu': ifu})
            
            if df.empty:
                return {
                    'found': False,
                    'data': [],
                    'summary': {},
                    'count': 0
                }
            
            # Convertir en dictionnaire
            data = df.to_dict('records')
            
            # TODO: Calculer les statistiques selon les colonnes réelles
            summary = {
                'total_records': len(df),
                'annees': df['ANNEE'].tolist() if 'ANNEE' in df.columns else []
            }
            
            return {
                'found': True,
                'data': data,
                'summary': summary,
                'count': len(data)
            }
            
        except Exception as e:
            logging.error(f"Erreur lors de la récupération des données SONABEL: {str(e)}")
            raise
        finally:
            if connection:
                connection.close()
                
    def get_contribuable_fournisseur_data(self, ifu):
        """Récupère les données de fournisseurs pour un IFU donné depuis la base Oracle
        Args:
            ifu: Numéro IFU du contribuable
        Returns:
            dict: Dictionnaire contenant {found, data, summary, count}
        """
        query =""" SELECT NUM_IFU_CLIENT, NUM_IFU_FOURN, ANNEE_FISCAL, MOIS_FISCAL,
       TVA_DEDUCTIBLE, TVA_FACTURE, ID_IMPOT, PR_HT, NATURE_DEDUCTION
FROM PROG_DCF.DCF_TVA_FACTURE_DEDUITE
WHERE NUM_IFU_FOURN = :ifu"""

        try:
            connection = self._get_connection().connect()
            df = pd.read_sql_query(query, connection, params={'ifu': ifu})
            
            if df.empty:
                return {
                    'found': False,
                    'data': [],
                    'summary': {},
                    'count': 0
                }
            df = df.replace({pd.NA: None, pd.NaT: None})
            df = df.where(pd.notnull(df), None)
            df = df.replace({np.nan: None})
            # Convertir en dictionnaire
            data = df.to_dict('records')

            summary = {
                'total_records': len(df),
                'annees': df['ANNEE_FISCAL'].tolist() if 'ANNEE_FISCAL' in df.columns else []
            }

            return {
                'found': True,
                'data': data,
                'summary': summary,
                'count': len(data)
            }

        except Exception as e:
            logging.error(f"Erreur lors de la récupération des données de fournisseurs: {str(e)}")
            raise
        finally:
            if connection:
                connection.close()
        
        
    def get_contribuable_client_data(self, ifu):
        """Récupère les données de clients pour un IFU donné depuis la base Oracle
        Args:
            ifu: Numéro IFU du contribuable
        Returns:
            dict: Dictionnaire contenant {found, data, summary, count}
        """
        query ="""
        SELECT NUM_IFU_CLIENT, NUM_IFU_FOURN, ANNEE_FISCAL, MOIS_FISCAL,
        TVA_DEDUCTIBLE, TVA_FACTURE, ID_IMPOT, PR_HT, NATURE_DEDUCTION
        FROM PROG_DCF.DCF_TVA_FACTURE_DEDUITE
        WHERE NUM_IFU_CLIENT = :ifu """

        try:
            connection = self._get_connection().connect()
            df = pd.read_sql_query(query, connection, params={'ifu': ifu})
            
            if df.empty:
                return {
                    'found': False,
                    'data': [],
                    'summary': {},
                    'count': 0
                }
            df = df.replace({pd.NA: None, pd.NaT: None})
            df = df.where(pd.notnull(df), None)
            df = df.replace({np.nan: None})
            # Convertir en dictionnaire
            data = df.to_dict('records')

            summary = {
                'total_records': len(df),
                'annees': df['ANNEE_FISCAL'].tolist() if 'ANNEE_FISCAL' in df.columns else []
            }

            return {
                'found': True,
                'data': data,
                'summary': summary,
                'count': len(data)
            }

        except Exception as e:
            logging.error(f"Erreur lors de la récupération des données de clients: {str(e)}")
            raise
        finally:
            if connection:
                connection.close()