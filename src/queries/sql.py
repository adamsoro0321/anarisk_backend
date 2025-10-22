from sqlalchemy import text

# 1.
sql_contribuable = text(""" 
    SELECT 
        c.ID_CONTR, c.NUM_IFU, c.NOM, c.NOM_MINEFID, c.DATE_CREATION, c.DATE_IMMAT, c.ETAT,
        c.TEL, c.EMAIL, c.CODE_FJ, c.CODE_REG_FISC, c.CODE_SECT_ACT, c.CAPITAL,
        c.CHIF_AFFAIREPREV, c.ADR_CTR, c.NATIONALITE_CTR, c.FLAG_RESIDENT,
        c.NOCNSS, c.FLAG_EMPLOYEUR, i.LIBELLE_DCI as STRUCTURES
    FROM SID_CONTRIBUABLE c
    JOIN SID_CONTRIBUABLE_IMMAT i ON c.NUM_IFU = i.NUMEROIFU
    WHERE c.ETAT = 'ACTIF' AND c.NUM_IFU IS NOT NULL 
    ORDER BY c.NUM_IFU  
""")

# 2.
sql_tva_complete = """
        WITH tva_base AS (
            SELECT 
                NUM_IFU, NOM, ANNEE_FISCAL, MOIS_FISCAL,
                MONTANT_DECLARE, 
                OP_TAXBLE_COURANTE_VENTES_PRESTATIONS_SERVICES_TRVX_IMMO_01,
                OP_TAXBLE_COURANTE_LIVRAISON_A_SOI_MEME_02, 
                OP_TAXBLE_COURANTE_CESSION_IMMO_03,
                OP_TAXBLE_COURANTE_OPERATION_TAXABLE_TAUX_10PC_218,
                OP_TAXBLE_COURANTE_AUTRES_OPERATIONS_TAXABLES_04,
                OP_TAXBLE_MARCHE_CDE_VENTES_05,
                OP_TAXBLE_MARCHE_CDE_PRESTATION_SERVICES_06,
                OP_TAXBLE_MARCHE_CDE_TRAVAUX_IMMO_TRVX_PUPLIC_07,
                OP_TAXBLE_MARCHE_CDE_10PC_220,
                OP_TAXBLE_MARCHE_CDE_AUTRES_OP_TAXABLES_08,
                OP_NTAXBLE_EXPORTATIONS_09,
                OP_NTAXBLE_AUTRES_COMM_EXTERIEUR_VTE_SUSPENSION_TAXE_10,
                OP_NTAXBLE_AUTRES_OP_NON_TAXABLE_11,
                TVA_AU_TAUX_185PC_12,
                SOUMISE_AU_TAUX_REDUIT_10PC_219,
                TVA_BRUTE_OMISE_REVERSER_13,
                TVA_ANT_DEDUITE_A_REVERSER_14,
                MONTANT_TOTAL_LA_BRUTE_15,
                TVA_DEDUCTIBLE_PÉRIODE_16,
                CREDIT_TVA_PERIODE_PRECEDENTE_17,
                CREDIT_TVA_DDE_REMBOURSEMENT_18,
                CREDIT_TVA_REMBOURSEMENT_NON_DEMANDE_19,
                TVA_VENTE_SERVICES_IMPAYES_CREANCE_IRECOUVRABLE_20,
                TVA_VTE_SERVICE_RESILIES_21,
                AUTRES_DEDUCTIONS_22,
                CREDIT_TVA_NON_REMBOURSE_23,
                MONTANT_TVA_PAYABLE_AVIS_CREDIT_127,
                MONTANT_TVA_NET_A_PAYER_25,
                MONTANT_TOTAL_OPERATIONS_24,
                CREDIT_TVA_A_REMBOURSER_26
            FROM PROG_DCF.DCF_ELT_LIQ_TVA
            WHERE ANNEE_FISCAL >= 2021 AND NUM_IFU IS NOT NULL
        ),
        tva_annuel AS (
            SELECT 
                NUM_IFU, NOM, ANNEE_FISCAL,
                SUM(COALESCE(MONTANT_DECLARE, 0)) as MONTANT_DECLARE,
                SUM(COALESCE(OP_TAXBLE_COURANTE_VENTES_PRESTATIONS_SERVICES_TRVX_IMMO_01, 0)) as OP_TAXBLE_COURANTE_VENTES_PRESTATIONS_SERVICES_TRVX_IMMO_01,
                SUM(COALESCE(OP_TAXBLE_COURANTE_LIVRAISON_A_SOI_MEME_02, 0)) as OP_TAXBLE_COURANTE_LIVRAISON_A_SOI_MEME_02,
                SUM(COALESCE(OP_TAXBLE_COURANTE_CESSION_IMMO_03, 0)) as OP_TAXBLE_COURANTE_CESSION_IMMO_03,
                SUM(COALESCE(OP_TAXBLE_COURANTE_OPERATION_TAXABLE_TAUX_10PC_218, 0)) as OP_TAXBLE_COURANTE_OPERATION_TAXABLE_TAUX_10PC_218,
                SUM(COALESCE(OP_TAXBLE_COURANTE_AUTRES_OPERATIONS_TAXABLES_04, 0)) as OP_TAXBLE_COURANTE_AUTRES_OPERATIONS_TAXABLES_04,
                SUM(COALESCE(OP_TAXBLE_MARCHE_CDE_VENTES_05, 0)) as OP_TAXBLE_MARCHE_CDE_VENTES_05,
                SUM(COALESCE(OP_TAXBLE_MARCHE_CDE_PRESTATION_SERVICES_06, 0)) as OP_TAXBLE_MARCHE_CDE_PRESTATION_SERVICES_06,
                SUM(COALESCE(OP_TAXBLE_MARCHE_CDE_TRAVAUX_IMMO_TRVX_PUPLIC_07, 0)) as OP_TAXBLE_MARCHE_CDE_TRAVAUX_IMMO_TRVX_PUPLIC_07,
                SUM(COALESCE(OP_TAXBLE_MARCHE_CDE_10PC_220, 0)) as OP_TAXBLE_MARCHE_CDE_10PC_220,
                SUM(COALESCE(OP_TAXBLE_MARCHE_CDE_AUTRES_OP_TAXABLES_08, 0)) as OP_TAXBLE_MARCHE_CDE_AUTRES_OP_TAXABLES_08,
                SUM(COALESCE(OP_NTAXBLE_EXPORTATIONS_09, 0)) as OP_NTAXBLE_EXPORTATIONS_09,
                SUM(COALESCE(OP_NTAXBLE_AUTRES_COMM_EXTERIEUR_VTE_SUSPENSION_TAXE_10, 0)) as OP_NTAXBLE_AUTRES_COMM_EXTERIEUR_VTE_SUSPENSION_TAXE_10,
                SUM(COALESCE(OP_NTAXBLE_AUTRES_OP_NON_TAXABLE_11, 0)) as OP_NTAXBLE_AUTRES_OP_NON_TAXABLE_11,
                SUM(COALESCE(TVA_AU_TAUX_185PC_12, 0)) as TVA_AU_TAUX_185PC_12,
                SUM(COALESCE(SOUMISE_AU_TAUX_REDUIT_10PC_219, 0)) as SOUMISE_AU_TAUX_REDUIT_10PC_219,
                SUM(COALESCE(TVA_BRUTE_OMISE_REVERSER_13, 0)) as TVA_BRUTE_OMISE_REVERSER_13,
                SUM(COALESCE(TVA_ANT_DEDUITE_A_REVERSER_14, 0)) as TVA_ANT_DEDUITE_A_REVERSER_14,
                SUM(COALESCE(MONTANT_TOTAL_LA_BRUTE_15, 0)) as MONTANT_TOTAL_LA_BRUTE_15,
                SUM(COALESCE(TVA_DEDUCTIBLE_PÉRIODE_16, 0)) as TVA_DEDUCTIBLE_PÉRIODE_16,
                SUM(COALESCE(CREDIT_TVA_PERIODE_PRECEDENTE_17, 0)) as CREDIT_TVA_PERIODE_PRECEDENTE_17,
                SUM(COALESCE(CREDIT_TVA_DDE_REMBOURSEMENT_18, 0)) as CREDIT_TVA_DDE_REMBOURSEMENT_18,
                SUM(COALESCE(CREDIT_TVA_REMBOURSEMENT_NON_DEMANDE_19, 0)) as CREDIT_TVA_REMBOURSEMENT_NON_DEMANDE_19,
                SUM(COALESCE(TVA_VENTE_SERVICES_IMPAYES_CREANCE_IRECOUVRABLE_20, 0)) as TVA_VENTE_SERVICES_IMPAYES_CREANCE_IRECOUVRABLE_20,
                SUM(COALESCE(TVA_VTE_SERVICE_RESILIES_21, 0)) as TVA_VTE_SERVICE_RESILIES_21,
                SUM(COALESCE(AUTRES_DEDUCTIONS_22, 0)) as AUTRES_DEDUCTIONS_22,
                SUM(COALESCE(CREDIT_TVA_NON_REMBOURSE_23, 0)) as CREDIT_TVA_NON_REMBOURSE_23,
                SUM(COALESCE(MONTANT_TVA_PAYABLE_AVIS_CREDIT_127, 0)) as MONTANT_TVA_PAYABLE_AVIS_CREDIT_127,
                SUM(COALESCE(MONTANT_TVA_NET_A_PAYER_25, 0)) as MONTANT_TVA_NET_A_PAYER_25,
                SUM(COALESCE(MONTANT_TOTAL_OPERATIONS_24, 0)) as MONTANT_TOTAL_OPERATIONS_24,
                SUM(COALESCE(CREDIT_TVA_A_REMBOURSER_26, 0)) as CREDIT_TVA_A_REMBOURSER_26,
                COUNT(*) as NB_DECLARATIONS
            FROM tva_base
            GROUP BY NUM_IFU, NOM, ANNEE_FISCAL
        ),
        tva_decembre AS (
            SELECT NUM_IFU, ANNEE_FISCAL,
                SUM(COALESCE(MONTANT_TVA_NET_A_PAYER_25, 0)) as TVA_DECEMBRE
            FROM tva_base WHERE MOIS_FISCAL = 12
            GROUP BY NUM_IFU, ANNEE_FISCAL
        )
        SELECT 
            ta.*,
            COALESCE(td.TVA_DECEMBRE, 0) as TVA_DECEMBRE
        FROM tva_annuel ta
        LEFT JOIN tva_decembre td ON ta.NUM_IFU = td.NUM_IFU AND ta.ANNEE_FISCAL = td.ANNEE_FISCAL
        ORDER BY ta.NUM_IFU, ta.ANNEE_FISCAL
        """

# 3.
sql_tva_deduction = """
            SELECT 
                NUM_IFU, NOM, ANNEE_FISCAL, MOIS_FISCAL,
                MONTANT_DECLARE, 
                OP_TAXBLE_COURANTE_VENTES_PRESTATIONS_SERVICES_TRVX_IMMO_01,
                OP_TAXBLE_COURANTE_LIVRAISON_A_SOI_MEME_02, 
                OP_TAXBLE_COURANTE_CESSION_IMMO_03,
                OP_NTAXBLE_EXPORTATIONS_09,
                OP_NTAXBLE_AUTRES_COMM_EXTERIEUR_VTE_SUSPENSION_TAXE_10,
                MONTANT_TVA_NET_A_PAYER_25,
                TVA_DEDUCTIBLE_PÉRIODE_16
            FROM PROG_DCF.DCF_ELT_LIQ_TVA
            WHERE ANNEE_FISCAL >= 2021 
            AND NUM_IFU IS NOT NULL 
            AND ROWNUM <= 100000
            ORDER BY ANNEE_FISCAL, MOIS_FISCAL, NUM_IFU
            """
# 4.
sql_dgd = """
SELECT 
    CODE_BUREAU, LIB_BUREAU, NUM_LIQUIDATION, DATE_LIQUIDATION, NUM_ARTICLE,
    NOMENCLATURE8, NOMENCLATURE10, LIBELLE, PAYS_ORIGINE, PAYS_DESTINATION1, PAYS_DESTINATION_FINALE,
    FLUX, REGIME, TYPE_DECLARATION, IFU, ENTREPRISE, LIBELLE_BUREAU_FRONTIERE,
    PDS_NET, PDS_BRT, QUANTITE, CAF, FOB, DATE_ENREGISTREMENT, COMPTE,
    LIBELLE_MODE_TRANSPORT, NUM_DECLARATION, DECLARANT, LIB_DECLARANT,
    DD, CSE, RS, TSB, TSC, TST, TIC, TVA, PCS, PC, TPP, CPV, IDR,
    IFU_DECLARANT, ACOMPTE, RSP, ETAT, ANNULEE, TEP, TPC, TVT
FROM SID_DGD_CPF 
WHERE IFU IS NOT NULL
  AND DATE_LIQUIDATION >= ADD_MONTHS(SYSDATE, -36)
ORDER BY IFU, DATE_LIQUIDATION
            """

# 5.
sql_programmations_control = """
            WITH vg_data AS (
                SELECT  
                    ID_CONTR, TYPE_CONTROLE, MAX(DATE_PROGR) as DATE_DERNIERE_VG
                FROM SID_PROGRAMME_VERIFICATION 
                WHERE TYPE_CONTROLE = 'GENERAL'
                GROUP BY ID_CONTR, TYPE_CONTROLE
            ),
            vp_data AS (
                SELECT  
                    ID_CONTR, TYPE_CONTROLE, MAX(DATE_PROGR) as DATE_DERNIERE_VP
                FROM SID_PROGRAMME_VERIFICATION 
                WHERE TYPE_CONTROLE != 'GENERAL'
                GROUP BY ID_CONTR, TYPE_CONTROLE
            ),
            prog_combined AS (
                SELECT 
                    COALESCE(vg.ID_CONTR, vp.ID_CONTR) as ID_CONTR,
                    vg.DATE_DERNIERE_VG,
                    vp.DATE_DERNIERE_VP
                FROM vg_data vg
                FULL OUTER JOIN vp_data vp ON vg.ID_CONTR = vp.ID_CONTR
            ),
            contrib_info AS (
                SELECT DISTINCT ID_CONTR, NUM_IFU, DATE_IMMAT, NOM_MINEFID 
                FROM SID_CONTRIBUABLE
                WHERE NUM_IFU IS NOT NULL
            ),
            avis_data AS (
                SELECT 
                    ID_CONTR, 
                    MAX(DATE_CORRESP) as DATE_DERNIERE_AVIS, 
                    MAX(CODE_GEST) as DERNIERE_GESTION_SOUMIS_VERIF,
                    COUNT(CODE_GEST) as NOMBRE_EXERC_SOUMIS_VERIF
                FROM SID_CORRESPONDANCE_BRIGADE
                WHERE CODE_TYP_COR = 4
                GROUP BY ID_CONTR
            )
            SELECT 
                ci.NUM_IFU, ci.DATE_IMMAT, ci.NOM_MINEFID,
                pc.DATE_DERNIERE_VG, pc.DATE_DERNIERE_VP,
                ad.DATE_DERNIERE_AVIS, ad.DERNIERE_GESTION_SOUMIS_VERIF
            FROM contrib_info ci
            LEFT JOIN prog_combined pc ON ci.ID_CONTR = pc.ID_CONTR
            LEFT JOIN avis_data ad ON ci.ID_CONTR = ad.ID_CONTR
            WHERE ci.NUM_IFU IS NOT NULL
            """

# 6.
sql_benefices_complete = """
            WITH ibnc_data AS (
                SELECT 
                    sid_contribuable.NUM_IFU, 
                    EXTRACT(YEAR FROM DATE_FIN_PERIODE) as ANNEE_FISCAL,
                    sid_element_liquidation.CODE_ELT_LIQ, 
                    sid_det_ele_liquidations.VALEUR_NUMERIQUE
                FROM sid_element_liquidation, sid_det_ele_liquidations,
                     sid_contribuable, sid_titre_recette, sid_impot, sid_periode_fiscale
                WHERE sid_det_ele_liquidations.CODE_ELT_LIQ = sid_element_liquidation.CODE_ELT_LIQ
                  AND sid_det_ele_liquidations.ID_IMPOT = sid_impot.ID_IMPOT
                  AND sid_impot.CODE_TITRE = sid_titre_recette.CODE_TITRE
                  AND sid_titre_recette.ID_CONTR = sid_contribuable.ID_CONTR
                  AND sid_det_ele_liquidations.CODE_NAT_IMP = '27'
                  AND sid_impot.ID_CAL_FISC = sid_periode_fiscale.CODE_CAL_FISC
                  AND sid_periode_fiscale.EXERCICE_GESTION > '2018'
                  AND sid_titre_recette.ETAT_ACTUEL != 'ANNULE'
            ),
            ibica_data AS (
                SELECT 
                    sid_contribuable.NUM_IFU, 
                    EXTRACT(YEAR FROM DATE_FIN_PERIODE) as ANNEE_FISCAL,
                    sid_element_liquidation.CODE_ELT_LIQ, 
                    sid_det_ele_liquidations.VALEUR_NUMERIQUE
                FROM sid_element_liquidation, sid_det_ele_liquidations,
                     sid_contribuable, sid_titre_recette, sid_impot, sid_periode_fiscale
                WHERE sid_det_ele_liquidations.CODE_ELT_LIQ = sid_element_liquidation.CODE_ELT_LIQ
                  AND sid_det_ele_liquidations.ID_IMPOT = sid_impot.ID_IMPOT
                  AND sid_impot.CODE_TITRE = sid_titre_recette.CODE_TITRE
                  AND sid_titre_recette.ID_CONTR = sid_contribuable.ID_CONTR
                  AND sid_det_ele_liquidations.CODE_NAT_IMP = '03'
                  AND sid_impot.ID_CAL_FISC = sid_periode_fiscale.CODE_CAL_FISC
                  AND sid_periode_fiscale.EXERCICE_GESTION > '2018'
                  AND sid_titre_recette.ETAT_ACTUEL != 'ANNULE'
            ),
            is_data AS (
                SELECT 
                    sid_contribuable.NUM_IFU, 
                    EXTRACT(YEAR FROM DATE_FIN_PERIODE) as ANNEE_FISCAL,
                    sid_element_liquidation.CODE_ELT_LIQ, 
                    sid_det_ele_liquidations.VALEUR_NUMERIQUE
                FROM sid_element_liquidation, sid_det_ele_liquidations,
                     sid_contribuable, sid_titre_recette, sid_impot, sid_periode_fiscale
                WHERE sid_det_ele_liquidations.CODE_ELT_LIQ = sid_element_liquidation.CODE_ELT_LIQ
                  AND sid_det_ele_liquidations.ID_IMPOT = sid_impot.ID_IMPOT
                  AND sid_impot.CODE_TITRE = sid_titre_recette.CODE_TITRE
                  AND sid_titre_recette.ID_CONTR = sid_contribuable.ID_CONTR
                  AND sid_det_ele_liquidations.CODE_NAT_IMP IN ('88', '111')
                  AND sid_impot.ID_CAL_FISC = sid_periode_fiscale.CODE_CAL_FISC
                  AND sid_periode_fiscale.EXERCICE_GESTION > '2018'
                  AND sid_titre_recette.ETAT_ACTUEL != 'ANNULE'
            )
            SELECT NUM_IFU, ANNEE_FISCAL, CODE_ELT_LIQ, VALEUR_NUMERIQUE, 'IBNC' as TYPE_BENEFICE
            FROM ibnc_data
            UNION ALL
            SELECT NUM_IFU, ANNEE_FISCAL, CODE_ELT_LIQ, VALEUR_NUMERIQUE, 'IBICA' as TYPE_BENEFICE
            FROM ibica_data
            UNION ALL
            SELECT NUM_IFU, ANNEE_FISCAL, CODE_ELT_LIQ, VALEUR_NUMERIQUE, 'IS' as TYPE_BENEFICE
            FROM is_data
            ORDER BY NUM_IFU, ANNEE_FISCAL, TYPE_BENEFICE
            """
# Note: Le script R n'utilise pas de table PROG_DCF.DCF_COMPTABILITE
# Il utilise le fichier Excel BASE_DONNEES.xlsx à la place
# Cette requête a été supprimée pour correspondre exactement au script R

sql_insd = """ 
SELECT 
     i."Numero_IFU",
    COALESCE(i."ID_INSD", r."ID_INSD", df."ID_INSD", p."ID_INSD", a."ID_INSD", ps."ID_INSD", dfin."ID_INSD", s."ID_INSD") AS ID_INSD,
    i."Annee",
     i."FDP_AcqApCreat", i."BLLDS_AcqApCreat", i."BLLDS_VirepostPost",
    r."XA_MargCommerc", r."XB_CA", r."XE_RESULT_EXPL",
    df."EtatTVA_AnneeN",
    p."TOTAL_PImmobilise", p."TOTAL_PVenduePays",
    a."AZ_TActifImmob_Brut", a."BI_Clients_Net",
    ps."DN_PRCCT_Net", ps."CP_TCPReA_Net",
    dfin."TPRC_AnneeN",
    s."FR_AnneeN",
    r."RK_ChargDePersonnel" ,
    r."RL_DotAmortProviDep" ,
    r.XH_RESULTAT_HAO 
FROM PROG_DCF.t_immobilisation i
LEFT JOIN PROG_DCF.t_resultat r 
    ON i."Numero_IFU" = r."Numero_IFU" AND i."Annee" = r."Annee"
LEFT JOIN PROG_DCF.t_dettesFiscales df 
    ON i."Numero_IFU" = df."Numero_IFU" AND i."Annee" = df."Annee"
LEFT JOIN PROG_DCF.t_production p 
    ON i."Numero_IFU" = p."Numero_IFU" AND i."Annee" = p."Annee"
LEFT JOIN PROG_DCF.t_actif a 
    ON i."Numero_IFU" = a."Numero_IFU" AND i."Annee" = a."Annee"
LEFT JOIN PROG_DCF.t_passif ps 
    ON i."Numero_IFU" = ps."Numero_IFU" AND i."Annee" = ps."Annee"
LEFT JOIN PROG_DCF.t_detteFinanciere dfin 
    ON i."Numero_IFU" = dfin."Numero_IFU" AND i."Annee" = dfin."Annee"
LEFT JOIN PROG_DCF.t_synthese s 
    ON i."Numero_IFU" = s."Numero_IFU" AND i."Annee" = s."Annee"
"""
