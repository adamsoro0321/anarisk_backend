from flask import Blueprint, jsonify, request, current_app
import pandas as pd
import numpy as np
import os
from services.contribuable_service import ContribuableService
from utils.util import get_latest_risk_file
from models.model import Quantume ,Programme, BRIGADES_AUTORISEES,Brigade
contribuable_bp = Blueprint('contribuable', __name__)
from extensions import db
from sqlalchemy.exc import IntegrityError
# ========== Chargement des données de risque ==========
_risk_file_name = get_latest_risk_file()
_risk_data_df = None
_risk_file_path = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    'data', 'risk_contribuables',
    _risk_file_name
) if _risk_file_name else None



def _load_risk_data():
    """Charge les données de risque depuis le fichier CSV"""
    global _risk_data_df
    if _risk_data_df is None or _risk_data_df.empty:
        if _risk_file_path and os.path.exists(_risk_file_path):
            try:
                _risk_data_df = pd.read_csv(_risk_file_path, sep=';', encoding='utf-8')
            except Exception as e:
                current_app.logger.error(f"Erreur chargement données risque: {e}")
    return _risk_data_df


def _safe_value(val):
    """Convertit les valeurs numpy/pandas non sérialisables en types Python natifs.
    NaN, NaT, None -> None (devient null en JSON, exploitable côté JS).
    numpy int/float -> int/float Python natifs.
    """
    if val is None:
        return None
    if isinstance(val, float) and (np.isnan(val) or np.isinf(val)):
        return None
    if isinstance(val, (np.integer,)):
        return int(val)
    if isinstance(val, (np.floating,)):
        f = float(val)
        return None if (np.isnan(f) or np.isinf(f)) else f
    if isinstance(val, (np.bool_,)):
        return bool(val)
    if pd.isna(val):
        return None
    return val

@contribuable_bp.route('/contribuables/<string:ifu>/douanes', methods=['GET'])
def get_douane_data_contribuables(ifu):
    """
    Récupère les données douanières pour un contribuable spécifique depuis la base Oracle
    Args:
        ifu: Numéro IFU du contribuable
    Returns:
        JSON avec les données douanières (import/export)
    """
    try:
        # Initialiser le service
        service = ContribuableService()
        
        # Récupérer les données via le service (requête base de données)
        result = service.get_douane_data(ifu)
        
        if not result['found']:
            return jsonify({
                'success': True,
                'message': 'Aucune donnée douanière trouvée pour ce contribuable',
                'ifu': ifu,
                'data': []
            }), 200
        
        return jsonify({
            'success': True,
            'ifu': ifu,
            'count': result['count'],
            'data': result['data'],
            'summary': result['summary']
        }), 200
        
    except Exception as e:
        current_app.logger.error(f"Erreur lors de la récupération des données douanières: {str(e)}")
        return jsonify({
            'success': False,
            'message': f'Erreur lors de la récupération des données: {str(e)}',
            'data': []
        }), 500


@contribuable_bp.route('/contribuables/<string:ifu>/insd', methods=['GET'])
def get_insd_data_contribuables(ifu):
    """
    Récupère les données INSD pour un contribuable depuis la base Oracle
    Args:
        ifu: Numéro IFU du contribuable
    Returns:
        JSON avec les données INSD (données comptables et statistiques)
    """
    try:
        # Initialiser le service
        service = ContribuableService()
        
        # Récupérer les données via le service (requête base de données)
        result = service.get_insd_data(ifu)
        
        if not result['found']:
            return jsonify({
                'success': True,
                'message': 'Aucune donnée INSD trouvée pour ce contribuable',
                'ifu': ifu,
                'data': []
            }), 200
        
        return jsonify({
            'success': True,
            'ifu': ifu,
            'count': result['count'],
            'data': result['data'],
            'summary': result['summary']
        }), 200
        
    except Exception as e:
        current_app.logger.error(f"Erreur lors de la récupération des données INSD: {str(e)}")
        return jsonify({
            'success': False,
            'message': f'Erreur lors de la récupération des données: {str(e)}',
            'data': []
        }), 500


@contribuable_bp.route('/contribuables/<string:ifu>/sonabel', methods=['GET'])
def get_sonabel_data_contribuables(ifu):
    """
    Récupère les données SONABEL pour un contribuable depuis la base Oracle
    Args:
        ifu: Numéro IFU du contribuable
    Returns:
        JSON avec les données SONABEL (consommation électrique)
    """
    try:
        # Initialiser le service
        service = ContribuableService()
        
        # Récupérer les données via le service (requête base de données)
        result = service.get_sonabel_data(ifu)
        
        if not result['found']:
            return jsonify({
                'success': True,
                'message': 'Aucune donnée SONABEL trouvée pour ce contribuable',
                'ifu': ifu,
                'data': []
            }), 200
        
        return jsonify({
            'success': True,
            'ifu': ifu,
            'count': result['count'],
            'data': result['data'],
            'summary': result['summary']
        }), 200
        
    except Exception as e:
        current_app.logger.error(f"Erreur lors de la récupération des données SONABEL: {str(e)}")
        return jsonify({
            'success': False,
            'message': f'Erreur lors de la récupération des données: {str(e)}',
            'data': []
        }), 500


@contribuable_bp.route('/contribuables/<string:ifu>/indicateurs', methods=['GET'])
def get_contribuable_indicateurs(ifu):
    """
    Récupère les indicateurs de risque d'un contribuable par année.
    
    Path Parameters:
        - ifu (str): Numéro IFU du contribuable
    
    Query Parameters:
        - annee (int): Année spécifique (optionnel, retourne toutes les années si absent)
    
    Returns:
        JSON structuré par année avec pour chaque indicateur : risque, score, gap
    """
    try:
        df = _load_risk_data()
        
        if df is None or df.empty:
            return jsonify({
                'success': False,
                'message': 'Données de risque non disponibles',
                'error_code': 'DATA_NOT_AVAILABLE'
            }), 404
        
        # Rechercher la colonne IFU
        ifu_column = None
        for col in ['NUM_IFU', 'IFU', 'IDENTIFIANT', 'ifu', 'num_ifu']:
            if col in df.columns:
                ifu_column = col
                break
        
        if not ifu_column:
            return jsonify({
                'success': False,
                'message': 'Colonne IFU non trouvée dans les données',
                'error_code': 'IFU_COLUMN_NOT_FOUND'
            }), 400
        
        # Filtrer par IFU
        df_contribuable = df[df[ifu_column].astype(str) == str(ifu)]
        
        if df_contribuable.empty:
            return jsonify({
                'success': False,
                'message': f'Contribuable avec IFU "{ifu}" non trouvé',
                'error_code': 'CONTRIBUABLE_NOT_FOUND'
            }), 404
        
        # Déterminer la colonne année
        annee_column = None
        for col in ['ANNEE_FISCAL', 'ANNEE', 'YEAR']:
            if col in df.columns:
                annee_column = col
                break
        
        # Filtrer par année si spécifié
        annee_param = request.args.get('annee', None)
        if annee_param and annee_column:
            df_contribuable = df_contribuable[
                df_contribuable[annee_column].astype(str) == str(annee_param)
            ]
            if df_contribuable.empty:
                return jsonify({
                    'success': False,
                    'message': f'Aucune donnée pour l\'année {annee_param}',
                    'error_code': 'NO_DATA_FOR_YEAR'
                }), 404
        
        # Extraire les informations de base du contribuable
        first_row = df_contribuable.iloc[0]
        info_columns = [
            'NUM_IFU', 'NOM_MINEFID', 'RAISON_SOCIALE', 'ETAT',
            'CODE_SECT_ACT', 'CODE_REG_FISC', 'STRUCTURES',
            'REGIME_FISCALE', 'SECTEUR_ACTIVITE', 'FORME_JURIDIQUE',
            'DATE_DEBUT_ACTIVITE', 'DATE_DERNIERE_VG',
            'DATE_DERNIERE_VP', 'DATE_DERNIERE_AVIS'
        ]
        contribuable_info = {}
        for col in info_columns:
            if col in df.columns:
                val = _safe_value(first_row[col])
                contribuable_info[col] = str(val) if val is not None else None
        
        # Identifier les colonnes indicateurs
        risque_columns = sorted([col for col in df.columns if col.startswith('RISQUE_')])
        
        # Construire la réponse structurée par année
        annees_data = {}
        years_available = []
        
        for _, row in df_contribuable.iterrows():
            # Déterminer l'année
            annee = None
            if annee_column:
                val = _safe_value(row[annee_column])
                if val is not None:
                    try:
                        annee = int(float(val))
                    except (ValueError, TypeError):
                        annee = str(val)
            
            annee_key = str(annee) if annee else 'non_definie'
            if annee and annee not in years_available:
                years_available.append(annee)
            
            indicateurs = []
            for risque_col in risque_columns:
                ind_suffix = risque_col.replace('RISQUE_', '')
                score_col = f'SCORE_{ind_suffix}'
                gap_col = f'GAP_{ind_suffix}'
                ratio_col = f'RATIO_{ind_suffix}'
                age_col = f'AGE_MOIS_{ind_suffix}'
                
                # Valeur risque
                risque_val = _safe_value(row[risque_col])
                risque = str(risque_val) if risque_val is not None else None
                
                # Valeur score
                score = None
                if score_col in df.columns:
                    s = _safe_value(row[score_col])
                    if s is not None:
                        try:
                            score = float(s)
                        except (ValueError, TypeError):
                            score = None
                
                # Valeur gap (écart)
                gap = None
                if gap_col in df.columns:
                    g = _safe_value(row[gap_col])
                    if g is not None:
                        try:
                            gap = float(g)
                        except (ValueError, TypeError):
                            gap = None
                
                # Valeur ratio (si présent)
                ratio = None
                if ratio_col in df.columns:
                    r = _safe_value(row[ratio_col])
                    if r is not None:
                        try:
                            ratio = float(r)
                        except (ValueError, TypeError):
                            ratio = None
                
                # Valeur age mois (si présent, ex: IND_3)
                age_mois = None
                if age_col in df.columns:
                    a = _safe_value(row[age_col])
                    if a is not None:
                        try:
                            age_mois = float(a)
                        except (ValueError, TypeError):
                            age_mois = None
                
                indicateur = {
                    'indicateur': risque_col,
                    'risque': risque,
                    'score': score,
                    'gap': gap,
                }
                
                # Ajouter les champs optionnels s'ils existent
                if ratio is not None:
                    indicateur['ratio'] = ratio
                if age_mois is not None:
                    indicateur['age_mois'] = age_mois
                
                indicateurs.append(indicateur)
            
            annees_data[annee_key] = {
                'annee': annee,
                'indicateurs': indicateurs,
                'nb_indicateurs': len(indicateurs)
            }
        
        # Statistiques globales
        all_scores = []
        risk_counts = {'rouge': 0, 'orange': 0, 'jaune': 0, 'vert': 0, 'non_disponible': 0}
        
        # Calculer sur la dernière année disponible ou toutes
        for annee_key, annee_info in annees_data.items():
            for ind in annee_info['indicateurs']:
                if ind['score'] is not None:
                    all_scores.append(ind['score'])
                risque = (ind['risque'] or '').lower()
                if risque in risk_counts:
                    risk_counts[risque] += 1
                else:
                    risk_counts['non_disponible'] += 1
        
        score_total = sum(all_scores) if all_scores else 0
        years_available.sort()
        
        return jsonify({
            'success': True,
            'ifu': ifu,
            'contribuable': contribuable_info,
            'score_total': round(score_total, 2),
            'risk_counts': risk_counts,
            'years_available': years_available,
            'annees': annees_data,
            'nb_annees': len(annees_data),
            'nb_indicateurs_total': len(risque_columns)
        }), 200
        
    except Exception as e:
        current_app.logger.error(f"Erreur get_contribuable_indicateurs: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'message': 'Erreur interne du serveur',
            'error_code': 'INTERNAL_SERVER_ERROR',
            'details': str(e)
        }), 500

@contribuable_bp.route('/contribuables/<string:ifu>/fournisseurs', methods=['GET'])
def get_contribuable_fournisseur_data(ifu):
    """
    Récupère les données des fournisseurs pour un contribuable depuis la base Oracle
    Args:
        ifu: Numéro IFU du contribuable
    Returns:
        JSON avec les données des fournisseurs
    """
    try:
        # Initialiser le service
        service = ContribuableService()
        
        # Récupérer les données via le service (requête base de données)
        result = service.get_contribuable_fournisseur_data(ifu)
        
        if not result.get('found', False):
            return jsonify({
                'success': True,
                'message': 'Aucune donnée de fournisseurs trouvée pour ce contribuable',
                'ifu': ifu,
                'data': []
            }), 200
        
        return jsonify({
            'success': True,
            'ifu': ifu,
            'count': result.get('count', 0),
            'data': result.get('data', []),
            'summary': result.get('summary', {})
        }), 200
        
    except Exception as e:
        current_app.logger.error(f"Erreur lors de la récupération des données de fournisseurs: {str(e)}")
        return jsonify({
            'success': False,
            'message': f'Erreur lors de la récupération des données: {str(e)}',
            'data': []
        }), 500


@contribuable_bp.route('/contribuables/<string:ifu>/clients', methods=['GET'])
def get_contribuable_client_data(ifu):
    """
    Récupère les données des clients pour un contribuable depuis la base Oracle
    Args:
        ifu: Numéro IFU du contribuable
    Returns:
        JSON avec les données des clients
    """
    try:
        # Initialiser le service
        service = ContribuableService()
        
        # Récupérer les données via le service (requête base de données)
        result = service.get_contribuable_client_data(ifu)
        
        if not result.get('found', False):
            return jsonify({
                'success': True,
                'message': 'Aucune donnée de clients trouvée pour ce contribuable',
                'ifu': ifu,
                'data': []
            }), 200
        
        return jsonify({
            'success': True,
            'ifu': ifu,
            'count': result.get('count', 0),
            'data': result.get('data', []),
            'summary': result.get('summary', {})
        }), 200
        
    except Exception as e:
        current_app.logger.error(f"Erreur lors de la récupération des données de clients: {str(e)}")
        return jsonify({
            'success': False,
            'message': f'Erreur lors de la récupération des données: {str(e)}',
            'data': []
        }), 500

@contribuable_bp.route('/contribuables/<string:ifu>/programmes', methods=['GET'])
def get_contribuable_programme(ifu):
    """
    Récupère tous les programmes associés à un contribuable (IFU)
    Args:
        ifu: Numéro IFU du contribuable
    Query Parameters:
        quantum (str): Filtre par libelle du quantum (optionnel, ex: q2_2026)
    Returns:
        JSON avec la liste des programmes (brigade, quantume, date_creation, actif)
    """
    try:
        # Récupérer le paramètre quantum optionnel
        quantum_param = request.args.get('quantum', None)

        # Construire la requête avec jointures sur Brigade et Quantume
        query = Programme.query.filter_by(IFU=ifu)

        # Filtrer par quantum (via la relation) si spécifié
        if quantum_param:
            query = query.join(Programme.quantume).filter(
                Quantume.libelle == quantum_param
            )

        # Exécuter la requête
        programmes = query.all()

        if not programmes:
            message = 'Aucun programme trouvé pour ce contribuable'
            if quantum_param:
                message += f' avec le quantum "{quantum_param}"'
            return jsonify({
                'success': True,
                'message': message,
                'ifu': ifu,
                'quantum': quantum_param,
                'data': []
            }), 200

        # Convertir les programmes en dictionnaire
        programmes_data = []
        quantumes_found = set()

        for prog in programmes:
            brigade_libelle = prog.brigade.libelle if prog.brigade else None
            quantume_libelle = prog.quantume.libelle if prog.quantume else None

            programmes_data.append({
                'id': prog.id,
                'ifu': prog.IFU,
                'brigade': brigade_libelle,
                'quantume': quantume_libelle,
                'actif': prog.actif,
                'date_creation': prog.date_creation.isoformat() if prog.date_creation else None
            })
            if quantume_libelle:
                quantumes_found.add(quantume_libelle)

        response = {
            'success': True,
            'ifu': ifu,
            'count': len(programmes_data),
            'data': programmes_data,
            'quantumes_trouves': sorted(list(quantumes_found))
        }

        if quantum_param:
            response['quantum_filtre'] = quantum_param

        return jsonify(response), 200

    except Exception as e:
        current_app.logger.error(f"Erreur lors de la récupération des programmes: {str(e)}")
        return jsonify({
            'success': False,
            'message': f'Erreur lors de la récupération des programmes: {str(e)}',
            'data': []
        }), 500

@contribuable_bp.route('/contribuables/programmes', methods=['POST'])
def create_programme():
    """Crée un nouveau programme d'analyse de risque pour un contribuable
    Args:
        ifu (str): Numéro IFU du contribuable
        brigade (str): Brigade d'analyse (doit être dans BRIGADES_AUTORISEES)
        quantume (str): Quantum d'analyse (ex: Q1_2026)
    Returns:
        dict: Résultat de la création avec succès ou message d'erreur
    """
    try:
        # Accepter JSON ou form-data
        if request.is_json:
            data = request.get_json()
        else:
            data = request.form.to_dict() or request.get_json(force=True, silent=True) or {}

        ifu = data.get('ifu')
        brigade = data.get('brigade')
        quantume = data.get('quantume')


        # Valider les champs obligatoires
        if not ifu or not brigade or not quantume:
            return jsonify({
                'success': False,
                'message': 'Les champs ifu, brigade et quantume sont obligatoires'
            }), 400

        # recuperer les id de brigade et quantume à partir des noms si pas fournis
        brigade_obj = Brigade.query.filter_by(libelle=brigade).first()
        if not brigade_obj:
            return jsonify({
                'success': False,
                'message': f'Brigade "{brigade}" non trouvée. Veuillez vérifier les brigades disponibles.'
            }), 400
        quantume_obj = Quantume.query.filter_by(libelle=quantume).first()
        if not quantume_obj:
            return jsonify({
                'success': False,
                'message': f'Quantum "{quantume}" non trouvé. Veuillez vérifier les quantums disponibles.'
            }), 400

        # Vérifier si un programme identique existe déjà (même IFU + brigade + quantume)
        existing = Programme.query.filter_by(
            IFU=ifu,
            id_brigade=brigade_obj.id,
            id_quantume=quantume_obj.id
        ).first()
        if existing:
            return jsonify({
                'success': False,
                'message': f'Un programme existe déjà pour cet IFU avec la brigade "{brigade}" et le quantum "{quantume}".'
            }), 409

        # Créer le programme
        new_programme = Programme(
            IFU=ifu,
            id_brigade= brigade_obj.id,
            id_quantume= quantume_obj.id,
            actif=True
        )
        db.session.add(new_programme)
        db.session.commit()
        return jsonify({
            'success': True,
            'message': 'Programme créé avec succès',
            'programme_id': new_programme.id
        }), 201
    except IntegrityError as e:
        db.session.rollback()
        current_app.logger.error(f"IntegrityError création programme: {str(e)}")
        return jsonify({
            'success': False,
            'message': 'Un programme identique existe déjà pour ce contribuable. '
                       'Si le problème persiste, la table doit être migrée (contrainte unique sur IFU à retirer).'
        }), 409
    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"Erreur lors de la création du programme: {str(e)}")
        return jsonify({
            'success': False,
            'message': f'Erreur lors de la création du programme: {str(e)}'
        }), 500
        
@contribuable_bp.route("/contribuables/fournisseurs", methods=["GET"])
def get_fournisseur():
    """Obtenir les fournisseurs d'un contribuable
    Query Parameters:
        ifu (str): Numéro IFU du contribuable
    Returns:
        JSON avec les données des fournisseurs
    """
    try:
        # Récupérer l'IFU depuis les paramètres de requête
        ifu = request.args.get('ifu')
        
        if not ifu:
            return jsonify({
                'success': False,
                'message': 'Le paramètre "ifu" est obligatoire'
            }), 400
        
        # Initialiser le service
        service = ContribuableService()
        
        # Récupérer les données via le service
        result = service.get_contribuable_fournisseur_data(ifu)
        
        if not result.get('found', False):
            return jsonify({
                'success': True,
                'message': 'Aucune donnée de fournisseurs trouvée pour ce contribuable',
                'ifu': ifu,
                'data': []
            }), 200
        
        return jsonify({
            'success': True,
            'ifu': ifu,
            'count': result.get('count', 0),
            'data': result.get('data', []),
            'summary': result.get('summary', {})
        }), 200
        
    except Exception as e:
        current_app.logger.error(f"Erreur lors de la récupération des fournisseurs: {str(e)}")
        return jsonify({
            'success': False,
            'message': f'Erreur lors de la récupération des données: {str(e)}',
            'data': []
        }), 500

@contribuable_bp.route("/contribuables/clients", methods=["GET"])
def get_client():
    """Obtenir les clients d'un contribuable
    Query Parameters:
        ifu (str): Numéro IFU du contribuable
    Returns:
        JSON avec les données des clients
    """
    try:
        # Récupérer l'IFU depuis les paramètres de requête
        ifu = request.args.get('ifu')
        
        if not ifu:
            return jsonify({
                'success': False,
                'message': 'Le paramètre "ifu" est obligatoire'
            }), 400
        
        # Initialiser le service
        service = ContribuableService()
        
        # Récupérer les données via le service
        result = service.get_contribuable_client_data(ifu)
        
        if not result.get('found', False):
            return jsonify({
                'success': True,
                'message': 'Aucune donnée de clients trouvée pour ce contribuable',
                'ifu': ifu,
                'data': []
            }), 200
        
        return jsonify({
            'success': True,
            'ifu': ifu,
            'count': result.get('count', 0),
            'data': result.get('data', []),
            'summary': result.get('summary', {})
        }), 200
        
    except Exception as e:
        current_app.logger.error(f"Erreur lors de la récupération des clients: {str(e)}")
        return jsonify({
            'success': False,
            'message': f'Erreur lors de la récupération des données: {str(e)}',
            'data': []
        }), 500