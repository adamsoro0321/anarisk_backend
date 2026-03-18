from flask import Blueprint, jsonify, request, current_app
from datetime import datetime
from sqlalchemy import or_, and_
from models import Indicateur
from extensions import db

indicateur_bp = Blueprint('indicateurs', __name__)


@indicateur_bp.route("/indicateurs", methods=['GET'])
def get_indicateurs():
    """Liste tous les indicateurs avec filtres optionnels (sans pagination)"""
    try:
        # Paramètres de filtrage
        actif = request.args.get('actif', type=str)
        type_controle = request.args.get('type_controle', type=str)
        impots_controle = request.args.get('impots_controle', type=str)
        implemente = request.args.get('implemente', type=str)
        criticite = request.args.get('criticite', type=str)
        
        # Construction de la requête
        query = Indicateur.query
        
        # Filtres
        if actif is not None:
            query = query.filter(Indicateur.actif == (actif.lower() == 'true'))
        if type_controle:
            query = query.filter(Indicateur.type_controle.ilike(f'%{type_controle}%'))
        if impots_controle:
            query = query.filter(Indicateur.impots_controle.ilike(f'%{impots_controle}%'))
        if implemente:
            query = query.filter(Indicateur.implemente.ilike(f'%{implemente}%'))
        if criticite:
            query = query.filter(Indicateur.criticite.ilike(f'%{criticite}%'))
        
        # Tri
        query = query.order_by(Indicateur.code_indicateur.asc())
        
        # Récupération de tous les résultats
        all_indicateurs = query.all()
        
        indicateurs = [{
            'id': i.id,
            'code_indicateur': i.code_indicateur,
            'intitule': i.intitule,
            'axes_controle': i.axes_controle,
            'objectif': i.objectif,
            'unite_mesure': i.unite_mesure,
            'variables_calcul': i.variables_calcul,
            'formule_calcul': i.formule_calcul,
            'seuil_declenchement': i.seuil_declenchement,
            'regle_selection': i.regle_selection,
            'criticite': i.criticite,
            'calcul_ecart': i.calcul_ecart,
            'coefficient_moderation': i.coefficient_moderation,
            'impact_recettes': i.impact_recettes,
            'designation_anomalie': i.designation_anomalie,
            'type_controle': i.type_controle,
            'sources_donnees': i.sources_donnees,
            'impots_controle': i.impots_controle,
            'segments_concernes': i.segments_concernes,
            'regimes_concernes': i.regimes_concernes,
            'forme_juridique': i.forme_juridique,
            'implemente': i.implemente,
            'limite': i.limite,
            'commentaires': i.commentaires,
            'type': i.type,
            'actif': i.actif,
            'date_creation': i.date_creation.isoformat() if i.date_creation else None,
            'date_modification': i.date_modification.isoformat() if i.date_modification else None
        } for i in all_indicateurs]
        
        return jsonify({
            'success': True,
            'data': indicateurs
        }), 200
        
    except Exception as e:
        current_app.logger.error(f"Erreur récupération indicateurs: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500


@indicateur_bp.route("/indicateurs/<int:id>", methods=['GET'])
def get_indicateur(id):
    """Récupère un indicateur spécifique par son ID"""
    try:
        indicateur = Indicateur.query.get_or_404(id)
        
        return jsonify({
            'success': True,
            'data': {
                'id': indicateur.id,
                'code_indicateur': indicateur.code_indicateur,
                'intitule': indicateur.intitule,
                'axes_controle': indicateur.axes_controle,
                'objectif': indicateur.objectif,
                'unite_mesure': indicateur.unite_mesure,
                'variables_calcul': indicateur.variables_calcul,
                'formule_calcul': indicateur.formule_calcul,
                'seuil_declenchement': indicateur.seuil_declenchement,
                'regle_selection': indicateur.regle_selection,
                'criticite': indicateur.criticite,
                'calcul_ecart': indicateur.calcul_ecart,
                'coefficient_moderation': indicateur.coefficient_moderation,
                'impact_recettes': indicateur.impact_recettes,
                'designation_anomalie': indicateur.designation_anomalie,
                'type_controle': indicateur.type_controle,
                'sources_donnees': indicateur.sources_donnees,
                'impots_controle': indicateur.impots_controle,
                'segments_concernes': indicateur.segments_concernes,
                'regimes_concernes': indicateur.regimes_concernes,
                'forme_juridique': indicateur.forme_juridique,
                'implemente': indicateur.implemente,
                'limite': indicateur.limite,
                'commentaires': indicateur.commentaires,
                'type': indicateur.type,
                'actif': indicateur.actif,
                'date_creation': indicateur.date_creation.isoformat() if indicateur.date_creation else None,
                'date_modification': indicateur.date_modification.isoformat() if indicateur.date_modification else None
            }
        }), 200
        
    except Exception as e:
        current_app.logger.error(f"Erreur récupération indicateur {id}: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500


@indicateur_bp.route("/indicateurs/code/<string:code>", methods=['GET'])
def get_indicateur_by_code(code):
    """Récupère un indicateur spécifique par son code"""
    try:
        indicateur = Indicateur.query.filter_by(code_indicateur=code).first_or_404()
        
        return jsonify({
            'success': True,
            'data': {
                'id': indicateur.id,
                'code_indicateur': indicateur.code_indicateur,
                'intitule': indicateur.intitule,
                'axes_controle': indicateur.axes_controle,
                'objectif': indicateur.objectif,
                'unite_mesure': indicateur.unite_mesure,
                'variables_calcul': indicateur.variables_calcul,
                'formule_calcul': indicateur.formule_calcul,
                'seuil_declenchement': indicateur.seuil_declenchement,
                'regle_selection': indicateur.regle_selection,
                'criticite': indicateur.criticite,
                'calcul_ecart': indicateur.calcul_ecart,
                'coefficient_moderation': indicateur.coefficient_moderation,
                'impact_recettes': indicateur.impact_recettes,
                'designation_anomalie': indicateur.designation_anomalie,
                'type_controle': indicateur.type_controle,
                'sources_donnees': indicateur.sources_donnees,
                'impots_controle': indicateur.impots_controle,
                'segments_concernes': indicateur.segments_concernes,
                'regimes_concernes': indicateur.regimes_concernes,
                'forme_juridique': indicateur.forme_juridique,
                'implemente': indicateur.implemente,
                'limite': indicateur.limite,
                'commentaires': indicateur.commentaires,
                'type': indicateur.type,
                'actif': indicateur.actif,
                'date_creation': indicateur.date_creation.isoformat() if indicateur.date_creation else None,
                'date_modification': indicateur.date_modification.isoformat() if indicateur.date_modification else None
            }
        }), 200
        
    except Exception as e:
        current_app.logger.error(f"Erreur récupération indicateur {code}: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500


@indicateur_bp.route("/indicateurs", methods=['POST'])
def create_indicateur():
    """Crée un nouvel indicateur"""
    try:
        data = request.get_json()
        
        # Validation des champs requis
        if not data.get('code_indicateur'):
            return jsonify({'success': False, 'message': 'Le code indicateur est requis'}), 400
        if not data.get('intitule'):
            return jsonify({'success': False, 'message': "L'intitulé est requis"}), 400
        
        # Vérification de l'unicité du code
        if Indicateur.query.filter_by(code_indicateur=data['code_indicateur']).first():
            return jsonify({'success': False, 'message': 'Un indicateur avec ce code existe déjà'}), 400
        
        # Création de l'indicateur
        indicateur = Indicateur(
            code_indicateur=data['code_indicateur'],
            intitule=data['intitule'],
            axes_controle=data.get('axes_controle'),
            objectif=data.get('objectif'),
            unite_mesure=data.get('unite_mesure'),
            variables_calcul=data.get('variables_calcul'),
            formule_calcul=data.get('formule_calcul'),
            seuil_declenchement=data.get('seuil_declenchement'),
            regle_selection=data.get('regle_selection'),
            criticite=data.get('criticite'),
            calcul_ecart=data.get('calcul_ecart'),
            coefficient_moderation=data.get('coefficient_moderation'),
            impact_recettes=data.get('impact_recettes'),
            designation_anomalie=data.get('designation_anomalie'),
            type_controle=data.get('type_controle'),
            sources_donnees=data.get('sources_donnees'),
            impots_controle=data.get('impots_controle'),
            segments_concernes=data.get('segments_concernes'),
            regimes_concernes=data.get('regimes_concernes'),
            forme_juridique=data.get('forme_juridique'),
            implemente=data.get('implemente'),
            limite=data.get('limite'),
            commentaires=data.get('commentaires'),
            type=data.get('type'),
            actif=data.get('actif', True)
        )
        
        db.session.add(indicateur)
        db.session.commit()
        
        return jsonify({
            'success': True,
            'message': 'Indicateur créé avec succès',
            'data': {
                'id': indicateur.id,
                'code_indicateur': indicateur.code_indicateur,
                'intitule': indicateur.intitule
            }
        }), 201
        
    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"Erreur création indicateur: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500


@indicateur_bp.route("/indicateurs/<int:id>", methods=['PUT'])
def update_indicateur(id):
    """Met à jour un indicateur existant"""
    try:
        indicateur = Indicateur.query.get_or_404(id)
        data = request.get_json()
        
        # Vérification de l'unicité du code si modifié
        if 'code_indicateur' in data and data['code_indicateur'] != indicateur.code_indicateur:
            if Indicateur.query.filter_by(code_indicateur=data['code_indicateur']).first():
                return jsonify({'success': False, 'message': 'Un indicateur avec ce code existe déjà'}), 400
        
        # Mise à jour des champs
        if 'code_indicateur' in data:
            indicateur.code_indicateur = data['code_indicateur']
        if 'intitule' in data:
            indicateur.intitule = data['intitule']
        if 'axes_controle' in data:
            indicateur.axes_controle = data['axes_controle']
        if 'objectif' in data:
            indicateur.objectif = data['objectif']
        if 'unite_mesure' in data:
            indicateur.unite_mesure = data['unite_mesure']
        if 'variables_calcul' in data:
            indicateur.variables_calcul = data['variables_calcul']
        if 'formule_calcul' in data:
            indicateur.formule_calcul = data['formule_calcul']
        if 'seuil_declenchement' in data:
            indicateur.seuil_declenchement = data['seuil_declenchement']
        if 'regle_selection' in data:
            indicateur.regle_selection = data['regle_selection']
        if 'criticite' in data:
            indicateur.criticite = data['criticite']
        if 'calcul_ecart' in data:
            indicateur.calcul_ecart = data['calcul_ecart']
        if 'coefficient_moderation' in data:
            indicateur.coefficient_moderation = data['coefficient_moderation']
        if 'impact_recettes' in data:
            indicateur.impact_recettes = data['impact_recettes']
        if 'designation_anomalie' in data:
            indicateur.designation_anomalie = data['designation_anomalie']
        if 'type_controle' in data:
            indicateur.type_controle = data['type_controle']
        if 'sources_donnees' in data:
            indicateur.sources_donnees = data['sources_donnees']
        if 'impots_controle' in data:
            indicateur.impots_controle = data['impots_controle']
        if 'segments_concernes' in data:
            indicateur.segments_concernes = data['segments_concernes']
        if 'regimes_concernes' in data:
            indicateur.regimes_concernes = data['regimes_concernes']
        if 'forme_juridique' in data:
            indicateur.forme_juridique = data['forme_juridique']
        if 'implemente' in data:
            indicateur.implemente = data['implemente']
        if 'limite' in data:
            indicateur.limite = data['limite']
        if 'commentaires' in data:
            indicateur.commentaires = data['commentaires']
        if 'type' in data:
            indicateur.type = data['type']
        if 'actif' in data:
            indicateur.actif = data['actif']
        
        indicateur.date_modification = datetime.now()
        
        db.session.commit()
        
        return jsonify({
            'success': True,
            'message': 'Indicateur mis à jour avec succès',
            'data': {
                'id': indicateur.id,
                'code_indicateur': indicateur.code_indicateur,
                'intitule': indicateur.intitule
            }
        }), 200
        
    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"Erreur mise à jour indicateur {id}: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500


@indicateur_bp.route("/indicateurs/<int:id>", methods=['DELETE'])
def delete_indicateur(id):
    """Supprime un indicateur (soft delete: désactivation)"""
    try:
        indicateur = Indicateur.query.get_or_404(id)
        
        # Soft delete: désactivation au lieu de suppression
        indicateur.actif = False
        indicateur.date_modification = datetime.now()
        
        db.session.commit()
        
        return jsonify({
            'success': True,
            'message': 'Indicateur désactivé avec succès'
        }), 200
        
    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"Erreur suppression indicateur {id}: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500


@indicateur_bp.route("/indicateurs/<int:id>/hard-delete", methods=['DELETE'])
def hard_delete_indicateur(id):
    """Supprime définitivement un indicateur"""
    try:
        indicateur = Indicateur.query.get_or_404(id)
        
        db.session.delete(indicateur)
        db.session.commit()
        
        return jsonify({
            'success': True,
            'message': 'Indicateur supprimé définitivement'
        }), 200
        
    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"Erreur suppression définitive indicateur {id}: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500


@indicateur_bp.route("/indicateurs/search", methods=['GET'])
def search_indicateurs():
    """Recherche d'indicateurs par mots-clés"""
    try:
        query_str = request.args.get('q', '', type=str)
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 20, type=int)
        
        if not query_str:
            return jsonify({'success': False, 'message': 'Paramètre de recherche requis'}), 400
        
        # Recherche dans plusieurs champs
        search_filter = or_(
            Indicateur.code_indicateur.ilike(f'%{query_str}%'),
            Indicateur.intitule.ilike(f'%{query_str}%'),
            Indicateur.objectif.ilike(f'%{query_str}%'),
            Indicateur.type_controle.ilike(f'%{query_str}%'),
            Indicateur.impots_controle.ilike(f'%{query_str}%'),
            Indicateur.designation_anomalie.ilike(f'%{query_str}%')
        )
        
        pagination = Indicateur.query.filter(search_filter).paginate(
            page=page, per_page=per_page, error_out=False
        )
        
        indicateurs = [{
            'id': i.id,
            'code_indicateur': i.code_indicateur,
            'intitule': i.intitule,
            'type_controle': i.type_controle,
            'impots_controle': i.impots_controle,
            'criticite': i.criticite,
            'implemente': i.implemente,
            'actif': i.actif
        } for i in pagination.items]
        
        return jsonify({
            'success': True,
            'data': indicateurs,
            'pagination': {
                'page': pagination.page,
                'per_page': pagination.per_page,
                'total': pagination.total,
                'pages': pagination.pages
            }
        }), 200
        
    except Exception as e:
        current_app.logger.error(f"Erreur recherche indicateurs: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500


@indicateur_bp.route("/indicateurs/stats", methods=['GET'])
def get_indicateurs_stats():
    """Récupère des statistiques sur les indicateurs"""
    try:
        total = Indicateur.query.count()
        actifs = Indicateur.query.filter_by(actif=True).count()
        inactifs = Indicateur.query.filter_by(actif=False).count()
        implementes = Indicateur.query.filter_by(implemente='oui').count()
        non_implementes = Indicateur.query.filter(
            or_(Indicateur.implemente == 'non', Indicateur.implemente.is_(None))
        ).count()
        
        # Statistiques par criticité
        criticites = db.session.query(
            Indicateur.criticite,
            db.func.count(Indicateur.id).label('count')
        ).group_by(Indicateur.criticite).all()
        
        # Statistiques par type de contrôle
        types_controle = db.session.query(
            Indicateur.type_controle,
            db.func.count(Indicateur.id).label('count')
        ).filter(Indicateur.type_controle.isnot(None)).group_by(Indicateur.type_controle).all()
        
        return jsonify({
            'success': True,
            'data': {
                'total': total,
                'actifs': actifs,
                'inactifs': inactifs,
                'implementes': implementes,
                'non_implementes': non_implementes,
                'par_criticite': {c[0] if c[0] else 'non_defini': c[1] for c in criticites},
                'par_type_controle': {t[0] if t[0] else 'non_defini': t[1] for t in types_controle}
            }
        }), 200
        
    except Exception as e:
        current_app.logger.error(f"Erreur statistiques indicateurs: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500


@indicateur_bp.route("/indicateurs/bulk", methods=['POST'])
def create_bulk_indicateurs():
    """Crée plusieurs indicateurs en masse"""
    try:
        data = request.get_json()
        
        if not isinstance(data, list):
            return jsonify({'success': False, 'message': 'Les données doivent être un tableau'}), 400
        
        created = []
        errors = []
        
        for idx, item in enumerate(data):
            try:
                # Validation
                if not item.get('code_indicateur') or not item.get('intitule'):
                    errors.append({
                        'index': idx,
                        'code': item.get('code_indicateur', 'N/A'),
                        'error': 'Code indicateur et intitulé requis'
                    })
                    continue
                
                # Vérification unicité
                if Indicateur.query.filter_by(code_indicateur=item['code_indicateur']).first():
                    errors.append({
                        'index': idx,
                        'code': item['code_indicateur'],
                        'error': 'Code indicateur existe déjà'
                    })
                    continue
                
                # Création
                indicateur = Indicateur(
                    code_indicateur=item['code_indicateur'],
                    intitule=item['intitule'],
                    axes_controle=item.get('axes_controle'),
                    objectif=item.get('objectif'),
                    unite_mesure=item.get('unite_mesure'),
                    variables_calcul=item.get('variables_calcul'),
                    formule_calcul=item.get('formule_calcul'),
                    seuil_declenchement=item.get('seuil_declenchement'),
                    regle_selection=item.get('regle_selection'),
                    criticite=item.get('criticite'),
                    calcul_ecart=item.get('calcul_ecart'),
                    coefficient_moderation=item.get('coefficient_moderation'),
                    impact_recettes=item.get('impact_recettes'),
                    designation_anomalie=item.get('designation_anomalie'),
                    type_controle=item.get('type_controle'),
                    sources_donnees=item.get('sources_donnees'),
                    impots_controle=item.get('impots_controle'),
                    segments_concernes=item.get('segments_concernes'),
                    regimes_concernes=item.get('regimes_concernes'),
                    forme_juridique=item.get('forme_juridique'),
                    implemente=item.get('implemente'),
                    limite=item.get('limite'),
                    commentaires=item.get('commentaires'),
                    type=item.get('type'),
                    actif=item.get('actif', True)
                )
                
                db.session.add(indicateur)
                created.append(item['code_indicateur'])
                
            except Exception as e:
                errors.append({
                    'index': idx,
                    'code': item.get('code_indicateur', 'N/A'),
                    'error': str(e)
                })
        
        if created:
            db.session.commit()
        
        return jsonify({
            'success': True,
            'message': f'{len(created)} indicateurs créés, {len(errors)} erreurs',
            'created': created,
            'errors': errors
        }), 201 if created else 400
        
    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"Erreur création bulk indicateurs: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500