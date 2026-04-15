"""
Blueprint pour les statistiques
Gère les endpoints de statistiques et d'analyse des risques
"""
from flask import Blueprint, jsonify, request, current_app
import pandas as pd
import os
from api.risk_data import get_risk_dataframe

stats_bp = Blueprint('stats', __name__)


@stats_bp.route('/stats', methods=['GET'])
def get_stats():
    """
    Renvoie les statistiques globales des données de risque
    
    Query Parameters:
        - libelle_quantume (str): Libellé du quantum (optionnel)
    
    Returns:
        JSON avec les statistiques agrégées
    """
    try:
        libelle_quantume = request.args.get('libelle_quantume', None)
        df = get_risk_dataframe(libelle_quantume)
        
        if df is None or df.empty:
            return jsonify({
                'success': False,
                'message': 'Données de risque non disponibles',
                'error_code': 'DATA_NOT_AVAILABLE'
            }), 404
        
        # Calcul des statistiques de base
        stats = {
            'total_contribuables': len(df),
            'columns': list(df.columns),
        }
        
        # Statistiques sur les scores si la colonne existe
        score_columns = [col for col in df.columns if 'SCORE' in col.upper()]
        if score_columns:
            stats['score_statistics'] = {}
            for col in score_columns:
                if df[col].dtype in ['int64', 'float64']:
                    stats['score_statistics'][col] = {
                        'min': float(df[col].min()) if pd.notna(df[col].min()) else None,
                        'max': float(df[col].max()) if pd.notna(df[col].max()) else None,
                        'mean': float(df[col].mean()) if pd.notna(df[col].mean()) else None,
                        'median': float(df[col].median()) if pd.notna(df[col].median()) else None,
                        'std': float(df[col].std()) if pd.notna(df[col].std()) else None
                    }
        
        # Distribution par niveau de risque si la colonne existe
        risk_level_columns = [col for col in df.columns if 'NIVEAU' in col.upper() or 'LEVEL' in col.upper() or 'RISK' in col.upper()]
        if risk_level_columns:
            stats['risk_distribution'] = {}
            for col in risk_level_columns:
                if df[col].dtype == 'object':
                    stats['risk_distribution'][col] = df[col].value_counts().to_dict()
        
        return jsonify({
            'success': True,
            'stats': stats
        }), 200
        
    except Exception as e:
        current_app.logger.error(f"Erreur dans get_stats: {str(e)}")
        return jsonify({
            'success': False,
            'message': 'Erreur interne du serveur',
            'error_code': 'INTERNAL_SERVER_ERROR'
        }), 500


@stats_bp.route('/stats/summary', methods=['GET'])
def get_summary():
    """
    Renvoie un résumé des données de risque
    
    Query Parameters:
        - libelle_quantume (str): Libellé du quantum (optionnel)
    
    Returns:
        JSON avec le résumé des données
    """
    try:
        libelle_quantume = request.args.get('libelle_quantume', None)
        df = get_risk_dataframe(libelle_quantume)
        
        if df is None or df.empty:
            return jsonify({
                'success': False,
                'message': 'Données de risque non disponibles',
                'error_code': 'DATA_NOT_AVAILABLE'
            }), 404
        
        # Résumé de base
        summary = {
            'total_records': len(df),
            'total_columns': len(df.columns),
            'memory_usage_mb': round(df.memory_usage(deep=True).sum() / 1024 / 1024, 2),
            'columns_info': []
        }
        
        # Informations sur chaque colonne
        for col in df.columns:
            col_info = {
                'name': col,
                'dtype': str(df[col].dtype),
                'non_null_count': int(df[col].count()),
                'null_count': int(df[col].isnull().sum()),
                'unique_count': int(df[col].nunique())
            }
            summary['columns_info'].append(col_info)
        
        return jsonify({
            'success': True,
            'summary': summary
        }), 200
        
    except Exception as e:
        current_app.logger.error(f"Erreur dans get_summary: {str(e)}")
        return jsonify({
            'success': False,
            'message': 'Erreur interne du serveur',
            'error_code': 'INTERNAL_SERVER_ERROR'
        }), 500


@stats_bp.route('/stats/top-risks', methods=['GET'])
def get_top_risks():
    """
    Renvoie les contribuables avec les scores de risque les plus élevés
    
    Query Parameters:
        - limit (int): Nombre de résultats (défaut: 10, max: 100)
        - score_column (str): Colonne de score à utiliser (optionnel)
    
    Returns:
        JSON avec les top contribuables à risque
    """
    try:
        df = get_risk_dataframe()
        
        if df is None or df.empty:
            return jsonify({
                'success': False,
                'message': 'Données de risque non disponibles',
                'error_code': 'DATA_NOT_AVAILABLE'
            }), 404
        
        # Paramètres
        try:
            limit = min(int(request.args.get('limit', 10)), 100)
        except ValueError:
            return jsonify({
                'success': False,
                'message': 'Le paramètre limit doit être un entier',
                'error_code': 'INVALID_LIMIT'
            }), 400
        
        score_column = request.args.get('score_column', None)
        
        # Trouver la colonne de score
        if score_column:
            if score_column not in df.columns:
                return jsonify({
                    'success': False,
                    'message': f'La colonne "{score_column}" n\'existe pas',
                    'available_columns': [col for col in df.columns if 'SCORE' in col.upper()],
                    'error_code': 'INVALID_COLUMN'
                }), 400
        else:
            # Chercher une colonne de score par défaut
            score_columns = [col for col in df.columns if 'SCORE' in col.upper() and 'TOTAL' in col.upper()]
            if not score_columns:
                score_columns = [col for col in df.columns if 'SCORE' in col.upper()]
            
            if score_columns:
                score_column = score_columns[0]
            else:
                return jsonify({
                    'success': False,
                    'message': 'Aucune colonne de score trouvée',
                    'error_code': 'NO_SCORE_COLUMN'
                }), 400
        
        # Trier et récupérer les top
        df_sorted = df.sort_values(by=score_column, ascending=False).head(limit)
        
        return jsonify({
            'success': True,
            'score_column_used': score_column,
            'count': len(df_sorted),
            'data': df_sorted.to_dict(orient='records')
        }), 200
        
    except Exception as e:
        current_app.logger.error(f"Erreur dans get_top_risks: {str(e)}")
        return jsonify({
            'success': False,
            'message': 'Erreur interne du serveur',
            'error_code': 'INTERNAL_SERVER_ERROR'
        }), 500


@stats_bp.route('/stats/contribuable/<ifu>', methods=['GET'])
def get_contribuable_indicators(ifu):
    """
    Récupère tous les indicateurs d'un contribuable spécifique
    
    Path Parameters:
        - ifu (str): Numéro IFU du contribuable
    
    Query Parameters:
        - annee (int): Année fiscale (optionnel, retourne toutes les années si non spécifié)
    
    Returns:
        JSON avec tous les indicateurs du contribuable
    """
    try:
        df = get_risk_dataframe()
        
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
        
        # Filtrer par année si spécifié
        annee = request.args.get('annee', None)
        if annee:
            annee_columns = [col for col in df.columns if 'ANNEE' in col.upper() or 'YEAR' in col.upper()]
            if annee_columns:
                df_contribuable = df_contribuable[df_contribuable[annee_columns[0]].astype(str) == str(annee)]
        
        if df_contribuable.empty:
            return jsonify({
                'success': False,
                'message': f'Aucune donnée pour l\'année {annee}',
                'error_code': 'NO_DATA_FOR_YEAR'
            }), 404
        
        # Extraire les informations de base
        first_row = df_contribuable.iloc[0]
        
        # Identifier les colonnes par catégorie
        info_columns = ['NUM_IFU', 'IFU', 'NOM_MINEFID', 'RAISON_SOCIALE', 'NOM', 
                        'ETAT', 'CODE_SECT_ACT', 'CODE_REG_FISC', 'STRUCTURES',
                        'DATE_DERNIERE_VG', 'DATE_DERNIERE_VP', 'DATE_DERNIERE_AVIS']
        
        risque_columns = [col for col in df.columns if col.startswith('RISQUE_')]
        score_columns = [col for col in df.columns if col.startswith('SCORE_')]
        gap_columns = [col for col in df.columns if col.startswith('GAP_')]
        
        # Construire les informations de base
        contribuable_info = {}
        for col in info_columns:
            if col in df.columns:
                val = first_row[col]
                contribuable_info[col] = None if pd.isna(val) else val
        
        # Construire les indicateurs avec leurs détails
        indicators = []
        processed_indicators = set()
        
        for risque_col in risque_columns:
            # Extraire le numéro/nom de l'indicateur (ex: RISQUE_IND_1 -> IND_1)
            ind_suffix = risque_col.replace('RISQUE_', '')
            
            if ind_suffix in processed_indicators:
                continue
            processed_indicators.add(ind_suffix)
            
            # Chercher les colonnes associées
            score_col = f'SCORE_{ind_suffix}'
            gap_col = f'GAP_{ind_suffix}'
            
            indicator = {
                'id': ind_suffix,
                'name': f'Indicateur {ind_suffix.replace("IND_", "")}',
                'risque': None,
                'score': None,
                'gap': None
            }
            
            # Récupérer les valeurs pour chaque année
            indicator_data = []
            for _, row in df_contribuable.iterrows():
                row_data = {
                    'annee': None,
                    'risque': None,
                    'score': None,
                    'gap': None
                }
                
                # Année
                annee_cols = [c for c in df.columns if 'ANNEE' in c.upper() and 'FISCAL' in c.upper()]
                if not annee_cols:
                    annee_cols = [c for c in df.columns if c.upper() == 'ANNEE']
                if annee_cols:
                    val = row[annee_cols[0]]
                    row_data['annee'] = int(val) if pd.notna(val) and val != '' else None
                
                # Risque
                if risque_col in df.columns:
                    val = row[risque_col]
                    row_data['risque'] = None if pd.isna(val) else str(val)
                
                # Score
                if score_col in df.columns:
                    val = row[score_col]
                    row_data['score'] = float(val) if pd.notna(val) and val != '' else None
                
                # Gap
                if gap_col in df.columns:
                    val = row[gap_col]
                    row_data['gap'] = float(val) if pd.notna(val) and val != '' else None
                
                indicator_data.append(row_data)
            
            indicator['data'] = indicator_data
            
            # Valeurs du dernier enregistrement pour l'aperçu
            if indicator_data:
                last_data = indicator_data[-1]
                indicator['risque'] = last_data['risque']
                indicator['score'] = last_data['score']
                indicator['gap'] = last_data['gap']
            
            indicators.append(indicator)
        
        # Calculer les statistiques globales
        total_scores = []
        for ind in indicators:
            if ind['score'] is not None:
                total_scores.append(ind['score'])
        
        score_total = sum(total_scores) if total_scores else 0
        
        # Compter les risques par couleur
        risk_counts = {'rouge': 0, 'jaune': 0, 'vert': 0, 'non_disponible': 0}
        for ind in indicators:
            risque = (ind['risque'] or '').lower()
            if risque == 'rouge':
                risk_counts['rouge'] += 1
            elif risque == 'jaune':
                risk_counts['jaune'] += 1
            elif risque == 'vert':
                risk_counts['vert'] += 1
            else:
                risk_counts['non_disponible'] += 1
        
        # Années disponibles
        years_available = sorted(list(set([
            d['annee'] for ind in indicators 
            for d in ind.get('data', []) 
            if d.get('annee') is not None
        ])))
        
        return jsonify({
            'success': True,
            'contribuable': {
                'ifu': ifu,
                'info': contribuable_info,
                'score_total': score_total,
                'risk_counts': risk_counts,
                'years_available': years_available,
                'indicators_count': len(indicators),
                'indicators': indicators,
                'records_count': len(df_contribuable)
            }
        }), 200
        
    except Exception as e:
        current_app.logger.error(f"Erreur dans get_contribuable_indicators: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'message': 'Erreur interne du serveur',
            'error_code': 'INTERNAL_SERVER_ERROR',
            'details': str(e)
        }), 500


@stats_bp.route('/stats/search', methods=['GET'])
def search_contribuables():
    """
    Recherche des contribuables par IFU ou nom
    
    Query Parameters:
        - q (str): Terme de recherche (IFU ou nom partiel)
        - limit (int): Nombre max de résultats (défaut: 20)
    
    Returns:
        JSON avec la liste des contribuables correspondants
    """
    try:
        df = get_risk_dataframe()
        
        if df is None or df.empty:
            return jsonify({
                'success': False,
                'message': 'Données de risque non disponibles',
                'error_code': 'DATA_NOT_AVAILABLE'
            }), 404
        
        query = request.args.get('q', '').strip()
        if not query or len(query) < 2:
            return jsonify({
                'success': False,
                'message': 'Le terme de recherche doit contenir au moins 2 caractères',
                'error_code': 'QUERY_TOO_SHORT'
            }), 400
        
        try:
            limit = min(int(request.args.get('limit', 20)), 50)
        except ValueError:
            limit = 20
        
        # Identifier les colonnes
        ifu_col = next((c for c in ['NUM_IFU', 'IFU', 'ifu'] if c in df.columns), None)
        nom_col = next((c for c in ['NOM_MINEFID', 'RAISON_SOCIALE', 'NOM', 'nom'] if c in df.columns), None)
        
        if not ifu_col:
            return jsonify({
                'success': False,
                'message': 'Colonne IFU non trouvée',
                'error_code': 'IFU_COLUMN_NOT_FOUND'
            }), 400
        
        # Recherche
        query_upper = query.upper()
        mask = df[ifu_col].astype(str).str.upper().str.contains(query_upper, na=False)
        
        if nom_col:
            mask = mask | df[nom_col].astype(str).str.upper().str.contains(query_upper, na=False)
        
        df_results = df[mask].drop_duplicates(subset=[ifu_col]).head(limit)
        
        # Construire la réponse
        results = []
        for _, row in df_results.iterrows():
            result = {
                'ifu': str(row[ifu_col]),
                'nom': str(row[nom_col]) if nom_col and pd.notna(row[nom_col]) else None,
            }
            
            # Ajouter d'autres infos si disponibles
            if 'STRUCTURES' in df.columns and pd.notna(row['STRUCTURES']):
                result['structure'] = str(row['STRUCTURES'])
            if 'ETAT' in df.columns and pd.notna(row['ETAT']):
                result['etat'] = str(row['ETAT'])
            
            results.append(result)
        
        return jsonify({
            'success': True,
            'query': query,
            'count': len(results),
            'results': results
        }), 200
        
    except Exception as e:
        current_app.logger.error(f"Erreur dans search_contribuables: {str(e)}")
        return jsonify({
            'success': False,
            'message': 'Erreur interne du serveur',
            'error_code': 'INTERNAL_SERVER_ERROR'
        }), 500


@stats_bp.route('/stats/indicator/<indicator_id>', methods=['GET'])
def get_indicator_distribution(indicator_id):
    """
    Récupère la distribution des niveaux de risque pour un indicateur spécifique
    
    Path Parameters:
        - indicator_id (str): ID de l'indicateur (ex: IND_1, IND_2, etc.)
    
    Query Parameters:
        - annee (int): Année fiscale (optionnel, filtre par année)
        - structure (str): Code structure (optionnel, filtre par structure)
        - libelle_quantume (str): Libellé du quantum (optionnel)
    
    Returns:
        JSON avec la distribution des risques pour l'indicateur
    """
    try:
        libelle_quantume = request.args.get('libelle_quantume', None)
        df = get_risk_dataframe(libelle_quantume)
        
        if df is None or df.empty:
            return jsonify({
                'success': False,
                'message': 'Données de risque non disponibles',
                'error_code': 'DATA_NOT_AVAILABLE'
            }), 404
        
        # Normaliser l'ID de l'indicateur
        indicator_id_upper = indicator_id.upper()
        if not indicator_id_upper.startswith('IND_'):
            indicator_id_upper = f'IND_{indicator_id_upper}'
        
        # Construire le nom de la colonne de risque
        risque_col = f'RISQUE_{indicator_id_upper}'
        score_col = f'SCORE_{indicator_id_upper}'
        gap_col = f'GAP_{indicator_id_upper}'
        
        # Vérifier si la colonne existe
        if risque_col not in df.columns:
            # Lister les indicateurs disponibles
            available_indicators = [
                col.replace('RISQUE_', '') 
                for col in df.columns 
                if col.startswith('RISQUE_IND_')
            ]
            return jsonify({
                'success': False,
                'message': f'Indicateur "{indicator_id}" non trouvé',
                'available_indicators': available_indicators,
                'error_code': 'INDICATOR_NOT_FOUND'
            }), 404
        
        # Appliquer les filtres optionnels
        df_filtered = df.copy()
        
        # Filtre par année
        annee = request.args.get('annee', None)
        if annee:
            annee_cols = [c for c in df.columns if 'ANNEE' in c.upper() and 'FISCAL' in c.upper()]
            if not annee_cols:
                annee_cols = [c for c in df.columns if c.upper() == 'ANNEE']
            if annee_cols:
                df_filtered = df_filtered[df_filtered[annee_cols[0]].astype(str) == str(annee)]
        
        # Filtre par structure
        structure = request.args.get('structure', None)
        if structure and 'STRUCTURES' in df.columns:
            df_filtered = df_filtered[
                df_filtered['STRUCTURES'].astype(str).str.upper().str.contains(structure.upper(), na=False)
            ]
        
        if df_filtered.empty:
            return jsonify({
                'success': False,
                'message': 'Aucune donnée après filtrage',
                'error_code': 'NO_DATA_AFTER_FILTER'
            }), 404
        
        # Calculer la distribution des risques
        risk_distribution = df_filtered[risque_col].value_counts().to_dict()
        
        # Normaliser les clés (minuscules)
        normalized_distribution = {}
        for key, value in risk_distribution.items():
            key_lower = str(key).lower().strip()
            if key_lower in ['rouge', 'red', 'élevé', 'eleve', 'high']:
                normalized_distribution['rouge'] = normalized_distribution.get('rouge', 0) + int(value)
            elif key_lower in ['jaune', 'yellow', 'moyen', 'medium']:
                normalized_distribution['jaune'] = normalized_distribution.get('jaune', 0) + int(value)
            elif key_lower in ['vert', 'green', 'faible', 'low']:
                normalized_distribution['vert'] = normalized_distribution.get('vert', 0) + int(value)
            elif key_lower in ['non disponible', 'n/a', 'na', 'nan', '']:
                normalized_distribution['non_disponible'] = normalized_distribution.get('non_disponible', 0) + int(value)
            else:
                normalized_distribution['autre'] = normalized_distribution.get('autre', 0) + int(value)
        
        # Calculer les totaux
        total_contribuables = len(df_filtered)
        total_evaluated = total_contribuables - normalized_distribution.get('non_disponible', 0) - normalized_distribution.get('autre', 0)
        
        # Calculer les statistiques de score si disponible
        score_stats = None
        if score_col in df.columns:
            score_series = pd.to_numeric(df_filtered[score_col], errors='coerce')
            score_valid = score_series.dropna()
            if len(score_valid) > 0:
                score_stats = {
                    'min': float(score_valid.min()),
                    'max': float(score_valid.max()),
                    'mean': float(score_valid.mean()),
                    'median': float(score_valid.median()),
                    'std': float(score_valid.std()) if len(score_valid) > 1 else 0
                }
        
        # Calculer les statistiques de gap si disponible
        gap_stats = None
        if gap_col in df.columns:
            gap_series = pd.to_numeric(df_filtered[gap_col], errors='coerce')
            gap_valid = gap_series.dropna()
            gap_valid = gap_valid[gap_valid > 0]  # Exclure les gaps nuls
            if len(gap_valid) > 0:
                gap_stats = {
                    'min': float(gap_valid.min()),
                    'max': float(gap_valid.max()),
                    'mean': float(gap_valid.mean()),
                    'sum': float(gap_valid.sum()),
                    'count': int(len(gap_valid))
                }
        
        # Calculer les pourcentages
        percentages = {}
        if total_evaluated > 0:
            for key in ['rouge', 'jaune', 'vert']:
                count = normalized_distribution.get(key, 0)
                percentages[key] = round((count / total_evaluated) * 100, 2)
        
        # Identifier la colonne IFU
        ifu_col = next((c for c in ['NUM_IFU', 'IFU', 'ifu'] if c in df.columns), None)
        nom_col = next((c for c in ['NOM_MINEFID', 'RAISON_SOCIALE', 'NOM'] if c in df.columns), None)
        
        # Top contribuables à risque pour cet indicateur
        top_risks = []
        if score_col in df.columns and ifu_col:
            df_with_score = df_filtered[pd.to_numeric(df_filtered[score_col], errors='coerce') > 0].copy()
            df_with_score['_score_num'] = pd.to_numeric(df_with_score[score_col], errors='coerce')
            df_sorted = df_with_score.nlargest(10, '_score_num')
            
            for _, row in df_sorted.iterrows():
                top_risk = {
                    'ifu': str(row[ifu_col]),
                    'nom': str(row[nom_col]) if nom_col and pd.notna(row[nom_col]) else None,
                    'risque': str(row[risque_col]) if pd.notna(row[risque_col]) else None,
                    'score': float(row['_score_num']) if pd.notna(row['_score_num']) else None,
                }
                if gap_col in df.columns and pd.notna(row[gap_col]):
                    gap_val = pd.to_numeric(row[gap_col], errors='coerce')
                    top_risk['gap'] = float(gap_val) if pd.notna(gap_val) else None
                top_risks.append(top_risk)
        
        return jsonify({
            'success': True,
            'indicator': {
                'id': indicator_id_upper,
                'risque_column': risque_col,
                'score_column': score_col if score_col in df.columns else None,
                'gap_column': gap_col if gap_col in df.columns else None
            },
            'filters': {
                'annee': annee,
                'structure': structure
            },
            'distribution': {
                'counts': normalized_distribution,
                'percentages': percentages,
                'total_contribuables': total_contribuables,
                'total_evaluated': total_evaluated
            },
            'score_stats': score_stats,
            'gap_stats': gap_stats,
            'top_risks': top_risks
        }), 200
        
    except Exception as e:
        current_app.logger.error(f"Erreur dans get_indicator_distribution: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'message': 'Erreur interne du serveur',
            'error_code': 'INTERNAL_SERVER_ERROR',
            'details': str(e)
        }), 500


@stats_bp.route('/stats/indicators', methods=['GET'])
def list_indicators():
    """
    Liste tous les indicateurs disponibles avec leurs statistiques de base
    
    Query Parameters:
        - libelle_quantume (str): Libellé du quantum (optionnel)
    
    Returns:
        JSON avec la liste des indicateurs
    """
    try:
        libelle_quantume = request.args.get('libelle_quantume', None)
        df = get_risk_dataframe(libelle_quantume)
        
        if df is None or df.empty:
            return jsonify({
                'success': False,
                'message': 'Données de risque non disponibles',
                'error_code': 'DATA_NOT_AVAILABLE'
            }), 404
        
        # Trouver toutes les colonnes de risque
        risque_columns = [col for col in df.columns if col.startswith('RISQUE_IND_')]
        
        indicators = []
        for risque_col in risque_columns:
            ind_id = risque_col.replace('RISQUE_', '')
            score_col = f'SCORE_{ind_id}'
            gap_col = f'GAP_{ind_id}'
            
            # Calculer la distribution rapide
            distribution = df[risque_col].value_counts().to_dict()
            
            # Compter par niveau
            rouge_count = 0
            jaune_count = 0
            vert_count = 0
            
            for key, value in distribution.items():
                key_lower = str(key).lower().strip()
                if key_lower in ['rouge', 'red', 'élevé', 'eleve', 'high']:
                    rouge_count += int(value)
                elif key_lower in ['jaune', 'yellow', 'moyen', 'medium']:
                    jaune_count += int(value)
                elif key_lower in ['vert', 'green', 'faible', 'low']:
                    vert_count += int(value)
            
            indicator = {
                'id': ind_id,
                'name': f'Indicateur {ind_id.replace("IND_", "")}',
                'has_score': score_col in df.columns,
                'has_gap': gap_col in df.columns,
                'distribution': {
                    'rouge': rouge_count,
                    'jaune': jaune_count,
                    'vert': vert_count
                },
                'total_evaluated': rouge_count + jaune_count + vert_count
            }
            
            indicators.append(indicator)
        
        # Trier par nombre de risques rouges (décroissant)
        indicators.sort(key=lambda x: x['distribution']['rouge'], reverse=True)
        
        return jsonify({
            'success': True,
            'count': len(indicators),
            'indicators': indicators
        }), 200
        
    except Exception as e:
        current_app.logger.error(f"Erreur dans list_indicators: {str(e)}")
        return jsonify({
            'success': False,
            'message': 'Erreur interne du serveur',
            'error_code': 'INTERNAL_SERVER_ERROR'
        }), 500


@stats_bp.route('/stats/risk-colors', methods=['GET'])
def get_risk_colors_distribution():
    """
    Endpoint pour obtenir le nombre de contribuables par couleur de risque (Rouge, Orange, Jaune, Vert)
    Agrège les données de tous les indicateurs
    
    Query Parameters:
        - libelle_quantume (str): Libellé du quantum (optionnel)
    
    Returns:
        JSON avec le comptage par couleur et statistiques détaillées
    """
    try:
        libelle_quantume = request.args.get('libelle_quantume', None)
        df = get_risk_dataframe(libelle_quantume)
        
        if df is None or df.empty:
            return jsonify({
                'success': False,
                'message': 'Données de risque non disponibles',
                'error_code': 'DATA_NOT_AVAILABLE'
            }), 404
        
        # Trouver toutes les colonnes RISQUE_IND_X
        risque_columns = [col for col in df.columns if col.startswith('RISQUE_IND_')]
        
        if not risque_columns:
            return jsonify({
                'success': False,
                'message': 'Aucune colonne de risque trouvée',
                'error_code': 'NO_RISK_COLUMNS'
            }), 404
        
        # Compteurs globaux par couleur
        global_counts = {
            'rouge': 0,
            'orange': 0,
            'jaune': 0,
            'vert': 0,
            'non_disponible': 0
        }
        
        # Compteurs par indicateur
        indicators_detail = {}
        
        # Pour chaque colonne de risque
        for col in risque_columns:
            # Extraire le numéro de l'indicateur
            indicator_id = col.replace('RISQUE_IND_', '')
            
            # Compter les valeurs pour chaque couleur
            value_counts = df[col].value_counts()
            
            rouge = int(value_counts.get('rouge', 0))
            orange = int(value_counts.get('orange', 0))
            jaune = int(value_counts.get('jaune', 0))
            vert = int(value_counts.get('vert', 0))
            non_dispo = int(value_counts.get('non_disponible', 0)) + int(df[col].isna().sum())
            
            # Ajouter aux compteurs globaux
            global_counts['rouge'] += rouge
            global_counts['orange'] += orange
            global_counts['jaune'] += jaune
            global_counts['vert'] += vert
            global_counts['non_disponible'] += non_dispo
            
            # Détail par indicateur
            indicators_detail[indicator_id] = {
                'column': col,
                'counts': {
                    'rouge': rouge,
                    'orange': orange,
                    'jaune': jaune,
                    'vert': vert,
                    'non_disponible': non_dispo
                },
                'total': rouge + orange + jaune + vert + non_dispo
            }
        
        # Calculer le total global
        total_global = sum(global_counts.values())
        
        # Calculer les pourcentages globaux
        global_percentages = {}
        for color, count in global_counts.items():
            global_percentages[color] = round((count / total_global * 100), 2) if total_global > 0 else 0
        
        # Nombre unique de contribuables par couleur dominante
        # Un contribuable est classé par sa couleur la plus critique (rouge > orange > jaune > vert)
        def get_dominant_color(row):
            for col in risque_columns:
                if row[col] == 'rouge':
                    return 'rouge'
            for col in risque_columns:
                if row[col] == 'orange':
                    return 'orange'
            for col in risque_columns:
                if row[col] == 'jaune':
                    return 'jaune'
            for col in risque_columns:
                if row[col] == 'vert':
                    return 'vert'
            return 'non_disponible'
        
        df_temp = df.copy()
        df_temp['dominant_color'] = df_temp.apply(get_dominant_color, axis=1)
        unique_counts = df_temp['dominant_color'].value_counts().to_dict()
        
        unique_contribuables = {
            'rouge': int(unique_counts.get('rouge', 0)),
            'orange': int(unique_counts.get('orange', 0)),
            'jaune': int(unique_counts.get('jaune', 0)),
            'vert': int(unique_counts.get('vert', 0)),
            'non_disponible': int(unique_counts.get('non_disponible', 0))
        }
        
        # Pourcentages des contribuables uniques
        total_contribuables = len(df)
        unique_percentages = {}
        for color, count in unique_contribuables.items():
            unique_percentages[color] = round((count / total_contribuables * 100), 2) if total_contribuables > 0 else 0
        
        return jsonify({
            'success': True,
            'total_contribuables': total_contribuables,
            'total_indicators': len(risque_columns),
            'global_distribution': {
                'counts': global_counts,
                'percentages': global_percentages,
                'total_evaluations': total_global
            },
            'unique_contribuables': {
                'counts': unique_contribuables,
                'percentages': unique_percentages,
                'description': 'Contribuables classés par leur couleur de risque la plus critique'
            },
            'by_indicator': indicators_detail
        }), 200
        
    except Exception as e:
        current_app.logger.error(f"Erreur dans get_risk_colors_distribution: {str(e)}")
        return jsonify({
            'success': False,
            'message': 'Erreur interne du serveur',
            'error_code': 'INTERNAL_SERVER_ERROR'
        }), 500

