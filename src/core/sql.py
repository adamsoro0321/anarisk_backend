from sqlalchemy import text

# 1.
sql_contribuable = text(""" 
  SELECT 
        c.ID_CONTR, c.NUM_IFU, c.NOM, c.NOM_MINEFID, c.DATE_CREATION, c.DATE_IMMAT, c.ETAT,
        c.TEL, c.EMAIL, c.CODE_FJ, c.CODE_REG_FISC, c.CODE_SECT_ACT, c.CAPITAL,
        c.CHIF_AFFAIREPREV, c.ADR_CTR, c.NATIONALITE_CTR, c.FLAG_RESIDENT,
        c.NOCNSS, c.FLAG_EMPLOYEUR, i.LIBELLE_DCI as STRUCTURES,
         da.LIBELLE_SECT_ACT,
         da.LIBELLE_GR_SECT_ACT
    FROM SID_CONTRIBUABLE c
    LEFT JOIN SID_CONTRIBUABLE_IMMAT i ON c.NUM_IFU = i.NUMEROIFU
    LEFT JOIN PROG_DCF.DCF_ACTIVITES da ON c.CODE_SECT_ACT = da.CODE_SECT_ACT 
    WHERE  c.NUM_IFU IS NOT NULL 
    ORDER BY c.NUM_IFU  
""")

# 2.
sql_tva_declaration = """
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
            FROM PROG_DCF.DCF_ELT_LIQ_TVA2
            -- WHERE ANNEE_FISCAL >= 2021 AND NUM_IFU IS NOT NULL --
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
            COALESCE(td.TVA_DECEMBRE, 0) as TVA_DECEMBRE,
            ta.ANNEE_FISCAL AS ANNEE
        FROM tva_annuel ta
        LEFT JOIN tva_decembre td ON ta.NUM_IFU = td.NUM_IFU AND ta.ANNEE_FISCAL = td.ANNEE_FISCAL
        ORDER BY ta.NUM_IFU, ta.ANNEE_FISCAL 
        """

# 3.
sql_tva_deduction = """
    WITH DCF_TVA_DEDUCTION AS (
    SELECT
        NUM_IFU_CLIENT,
        UPPER(REGEXP_REPLACE(REGEXP_REPLACE(NUM_IFU_FOURN, ' ', ''), '[^[:alnum:]]', '')) AS NUM_IFU_FOURN,
        ANNEE_FISCAL,
        MOIS_FISCAL,
        TVA_DEDUCTIBLE,
        TVA_FACTURE,
        PR_HT,
        NATURE_DEDUCTION
    FROM PROG_DCF.DCF_TVA_FACTURE_DEDUITE
),
AN_CLIENT_11_12 AS (
    SELECT
        NUM_IFU_CLIENT AS NUM_IFU,
        ANNEE_FISCAL AS ANNEE,
        SUM(TVA_DEDUCTIBLE) AS CLI_TVA_DEDUCTIBLE_NOV_DEC,
        SUM(PR_HT) AS CLI_PR_HT_NOV_DEC,
        SUM(TVA_FACTURE) AS CLI_TVA_FACTURE_NOV_DEC
    FROM DCF_TVA_DEDUCTION
    WHERE MOIS_FISCAL IN (11, 12)
    GROUP BY NUM_IFU_CLIENT, ANNEE_FISCAL
),
AN_FOURN_11_12 AS (
    SELECT
        NUM_IFU_FOURN AS NUM_IFU,
        ANNEE_FISCAL AS ANNEE,
        SUM(TVA_DEDUCTIBLE) AS FOURN_TVA_DEDUCTIBLE_NOV_DEC,
        SUM(PR_HT) AS FOURN_PR_HT_NOV_DEC,
        SUM(TVA_FACTURE) AS FOURN_TVA_FACTURE_NOV_DEC
    FROM DCF_TVA_DEDUCTION
    WHERE MOIS_FISCAL IN (11, 12)
    GROUP BY NUM_IFU_FOURN, ANNEE_FISCAL
),
AN_CLIENT AS (
    SELECT
        NUM_IFU_CLIENT AS NUM_IFU,
        ANNEE_FISCAL AS ANNEE,
        SUM(TVA_DEDUCTIBLE) AS CLI_TVA_DEDUCTIBLE_AN,
        SUM(PR_HT) AS CLI_PR_HT_AN,
        SUM(TVA_FACTURE) AS CLI_TVA_FACTURE_AN
    FROM DCF_TVA_DEDUCTION
    GROUP BY NUM_IFU_CLIENT, ANNEE_FISCAL
),
AN_FOURN AS (
    SELECT
        NUM_IFU_FOURN AS NUM_IFU,
        ANNEE_FISCAL AS ANNEE,
        SUM(TVA_DEDUCTIBLE) AS FOURN_TVA_DEDUCTIBLE_AN,
        SUM(PR_HT) AS FOURN_PR_HT_AN,
        SUM(TVA_FACTURE) AS FOURN_TVA_FACTURE_AN
    FROM DCF_TVA_DEDUCTION
    GROUP BY NUM_IFU_FOURN, ANNEE_FISCAL
),
DGD_IMPORT AS (
    SELECT
        NUM_IFU_CLIENT AS NUM_IFU,
        ANNEE_FISCAL AS ANNEE,
        SUM(TVA_DEDUCTIBLE) AS TVA_SUPPORTE_IMPORT
    FROM DCF_TVA_DEDUCTION
    WHERE NATURE_DEDUCTION IN ('BAIS Importation', 'IMM importation')
    GROUP BY NUM_IFU_CLIENT, ANNEE_FISCAL
),
BD_TVA_DEDUC AS (
    SELECT
        COALESCE(c11.NUM_IFU, f11.NUM_IFU, ca.NUM_IFU, fa.NUM_IFU) AS NUM_IFU,
        COALESCE(c11.ANNEE, f11.ANNEE, ca.ANNEE, fa.ANNEE) AS ANNEE,
        c11.CLI_TVA_DEDUCTIBLE_NOV_DEC,
        c11.CLI_PR_HT_NOV_DEC,
        c11.CLI_TVA_FACTURE_NOV_DEC,
        f11.FOURN_TVA_DEDUCTIBLE_NOV_DEC,
        f11.FOURN_PR_HT_NOV_DEC,
        f11.FOURN_TVA_FACTURE_NOV_DEC,
        ca.CLI_TVA_DEDUCTIBLE_AN,
        ca.CLI_PR_HT_AN,
        ca.CLI_TVA_FACTURE_AN,
        fa.FOURN_TVA_DEDUCTIBLE_AN,
        fa.FOURN_PR_HT_AN,
        fa.FOURN_TVA_FACTURE_AN
    FROM AN_CLIENT_11_12 c11
        FULL JOIN AN_FOURN_11_12 f11 
            ON c11.NUM_IFU = f11.NUM_IFU AND c11.ANNEE = f11.ANNEE
        FULL JOIN AN_CLIENT ca 
            ON ca.NUM_IFU = COALESCE(c11.NUM_IFU, f11.NUM_IFU)
           AND ca.ANNEE  = COALESCE(c11.ANNEE, f11.ANNEE)
        FULL JOIN AN_FOURN fa 
            ON fa.NUM_IFU = COALESCE(c11.NUM_IFU, f11.NUM_IFU, ca.NUM_IFU)
           AND fa.ANNEE  = COALESCE(c11.ANNEE, f11.ANNEE, ca.ANNEE)
)
SELECT
    b.NUM_IFU,
    b.ANNEE,
    NVL(b.CLI_TVA_DEDUCTIBLE_NOV_DEC, 0) AS CLI_TVA_DEDUCTIBLE_NOV_DEC,
    NVL(b.CLI_PR_HT_NOV_DEC, 0) AS CLI_PR_HT_NOV_DEC,
    NVL(b.CLI_TVA_FACTURE_NOV_DEC, 0) AS CLI_TVA_FACTURE_NOV_DEC,
    NVL(b.FOURN_TVA_DEDUCTIBLE_NOV_DEC, 0) AS FOURN_TVA_DEDUCTIBLE_NOV_DEC,
    NVL(b.FOURN_PR_HT_NOV_DEC, 0) AS FOURN_PR_HT_NOV_DEC,
    NVL(b.FOURN_TVA_FACTURE_NOV_DEC, 0) AS FOURN_TVA_FACTURE_NOV_DEC,
    NVL(b.CLI_TVA_DEDUCTIBLE_AN, 0) AS CLI_TVA_DEDUCTIBLE_AN,
    NVL(b.CLI_PR_HT_AN, 0) AS CLI_PR_HT_AN,
    NVL(b.CLI_TVA_FACTURE_AN, 0) AS CLI_TVA_FACTURE_AN,
    NVL(b.FOURN_TVA_DEDUCTIBLE_AN, 0) AS FOURN_TVA_DEDUCTIBLE_AN,
    NVL(b.FOURN_PR_HT_AN, 0) AS FOURN_PR_HT_AN,
    NVL(b.FOURN_TVA_FACTURE_AN, 0) AS FOURN_TVA_FACTURE_AN,
    NVL(i.TVA_SUPPORTE_IMPORT, 0) AS TVA_SUPPORTE_IMPORT
FROM BD_TVA_DEDUC b
LEFT JOIN DGD_IMPORT i 
    ON i.NUM_IFU = b.NUM_IFU AND i.ANNEE = b.ANNEE
            """
# 4.
sql_dgd = """
WITH DGD_PREP AS (
  SELECT
    D.IFU,
    D.FLUX,
    TO_CHAR(D.DATE_LIQUIDATION, 'YYYY') AS ANNEE,
    NVL(D.CAF, 0) AS CAF,
    NVL(D.FOB, 0) AS FOB,
    NVL(D.TVA, 0) AS TVA,
    LPAD(SUBSTR(D.NOMENCLATURE10, 1, 2), 2, '0') AS CODE_CHAPITRE,
    NSH.\"Code Titre\" AS CODE_TITRE
  FROM SID_DGD_CPF D
  LEFT JOIN ODS.NOMENCLATURE_SH NSH
    ON LPAD(SUBSTR(D.NOMENCLATURE10, 1, 2), 2, '0') = NSH.\"Code_Chapitre\"
),
AGG_MONTANTS AS (
  SELECT
    IFU,
    FLUX,
    ANNEE,
    SUM(CAF) AS SUM_CAF,
    SUM(FOB) AS SUM_FOB,
    SUM(TVA) AS SUM_TVA
  FROM DGD_PREP
  GROUP BY IFU, FLUX, ANNEE
),
IMPORTS AS (
  SELECT
    IFU AS NUM_IFU,
    ANNEE,
    SUM_CAF AS IMPORT_CAF,
    SUM_FOB AS IMPORT_FOB,
    SUM_TVA AS IMPORT_TVA
  FROM AGG_MONTANTS
  WHERE FLUX = 'I'
),
EXPORTS AS (
  SELECT
    IFU AS NUM_IFU,
    ANNEE,
    SUM_CAF AS EXPORT_CAF,
    SUM_FOB AS EXPORT_FOB,
    SUM_TVA AS EXPORT_TVA
  FROM AGG_MONTANTS
  WHERE FLUX = 'E'
),
NB_TITRES AS (
  SELECT
    IFU,
    FLUX,
    ANNEE,
    COUNT(DISTINCT CODE_TITRE) AS NB_TITRES
  FROM DGD_PREP
  WHERE CODE_TITRE IS NOT NULL
  GROUP BY IFU, FLUX, ANNEE
),
NB_IMPORTS AS (
  SELECT
    IFU AS NUM_IFU,
    ANNEE,
    NB_TITRES AS IMPORT_NOMBRE_TITRE
  FROM NB_TITRES
  WHERE FLUX = 'I'
),
NB_EXPORTS AS (
  SELECT
    IFU AS NUM_IFU,
    ANNEE,
    NB_TITRES AS EXPORT_NOMBRE_TITRE
  FROM NB_TITRES
  WHERE FLUX = 'E'
),
MERGE_MONTANTS AS (
  SELECT
    COALESCE(I.NUM_IFU, E.NUM_IFU) AS NUM_IFU,
    COALESCE(I.ANNEE, E.ANNEE) AS ANNEE,
    NVL(I.IMPORT_CAF, 0) AS IMPORT_CAF,
    NVL(I.IMPORT_FOB, 0) AS IMPORT_FOB,
    NVL(I.IMPORT_TVA, 0) AS IMPORT_TVA,
    NVL(E.EXPORT_CAF, 0) AS EXPORT_CAF,
    NVL(E.EXPORT_FOB, 0) AS EXPORT_FOB,
    NVL(E.EXPORT_TVA, 0) AS EXPORT_TVA
  FROM IMPORTS I
  FULL OUTER JOIN EXPORTS E ON I.NUM_IFU = E.NUM_IFU AND I.ANNEE = E.ANNEE
),
MERGE_TITRES AS (
  SELECT
    COALESCE(I.NUM_IFU, E.NUM_IFU) AS NUM_IFU,
    COALESCE(I.ANNEE, E.ANNEE) AS ANNEE,
    NVL(I.IMPORT_NOMBRE_TITRE, 0) AS IMPORT_NOMBRE_TITRE,
    NVL(E.EXPORT_NOMBRE_TITRE, 0) AS EXPORT_NOMBRE_TITRE
  FROM NB_IMPORTS I
  FULL OUTER JOIN NB_EXPORTS E ON I.NUM_IFU = E.NUM_IFU AND I.ANNEE = E.ANNEE
)
SELECT
  D.*,
  T.IMPORT_NOMBRE_TITRE,
  T.EXPORT_NOMBRE_TITRE
FROM MERGE_MONTANTS D
LEFT JOIN MERGE_TITRES T ON D.NUM_IFU = T.NUM_IFU AND D.ANNEE = T.ANNEE
            """

# 5.
sql_programmations_control = """
            WITH vg_data AS (
                SELECT  
                    ID_CONTR, TYPE_CONTROLE, MAX(DATE_PROGR) as DATE_DERNIERE_VG
                FROM SID_PROGRAMME_VERIFICATION 
                WHERE TYPE_CONTROLE = 'GENERAL'
                GROUP BY ID_CONTR, NUM_IFU, TYPE_CONTROLE
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
            ),
            programmation_with_ifu AS (
                SELECT 
                    pc.ID_CONTR,
                    ci.NUM_IFU,
                    ci.DATE_IMMAT,
                    pc.DATE_DERNIERE_VG,
                    pc.DATE_DERNIERE_VP,
                    ci.NOM_MINEFID
                FROM prog_combined pc
                LEFT JOIN contrib_info ci ON pc.ID_CONTR = ci.ID_CONTR
            )
            SELECT DISTINCT
                pwi.NUM_IFU, 
                pwi.DATE_IMMAT, 
                pwi.DATE_DERNIERE_VG, 
                pwi.DATE_DERNIERE_VP,
                pwi.NOM_MINEFID,
                ad.DATE_DERNIERE_AVIS, 
                ad.DERNIERE_GESTION_SOUMIS_VERIF
            FROM programmation_with_ifu pwi
            FULL OUTER JOIN avis_data ad ON pwi.ID_CONTR = ad.ID_CONTR
            WHERE pwi.NUM_IFU IS NOT NULL
            ORDER BY pwi.NUM_IFU
            """ 


# 6.
sql_benefices_complete = """
            WITH ibnc_pivot AS (
                SELECT * FROM (
                    SELECT sid_contribuable.NUM_IFU, 
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
                      AND sid_periode_fiscale.EXERCICE_GESTION > '2022'
                      AND sid_titre_recette.ETAT_ACTUEL != 'ANNULE'
                )
                PIVOT (
                    SUM(VALEUR_NUMERIQUE) 
                    FOR CODE_ELT_LIQ IN ('69' as CA_HTVA, '120' as RETENUE_SOURCE_IMPUTE, '115' as BENEFICE_IMPOSABLE, 
                                          '119' as IBENEF_EXIGIBLE, '122' as IBENEF_DUES, '123' as PREL_SOURCE_ACOMPTE, '228' as DEDUC_CGA)
                )
            ),
            ibnc_final AS (
                SELECT NUM_IFU, ANNEE_FISCAL, 
                       NVL(CA_HTVA, 0) as CA_HTVA, 
                       NVL(BENEFICE_IMPOSABLE, 0) as BENEFICE_IMPOSABLE,
                       NVL(IBENEF_EXIGIBLE, 0) as IBENEF_EXIGIBLE, 
                       NVL(IBENEF_DUES, 0) as IBENEF_DUES,
                       NVL(PREL_SOURCE_ACOMPTE, 0) as PREL_SOURCE_ACOMPTE, 
                       NVL(RETENUE_SOURCE_IMPUTE, 0) as RETENUE_SOURCE_IMPUTE
                FROM ibnc_pivot
            ),
            ibica_pivot AS (
                SELECT * FROM (
                    SELECT sid_contribuable.NUM_IFU, 
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
                      AND sid_periode_fiscale.EXERCICE_GESTION > '2022'
                      AND sid_titre_recette.ETAT_ACTUEL != 'ANNULE'
                )
                PIVOT (
                    SUM(VALEUR_NUMERIQUE) 
                    FOR CODE_ELT_LIQ IN ('69' as CA_HTVA, '121' as COTIS_BIC_PAYER, '116' as IBENEF_EXIGIBLE, 
                                          '118' as IBENEF_DUES, '120' as RETENUE_SOURCE_IMPUTE, '115' as BENEFICE_IMPOSABLE,
                                          '117' as COTIS_MFP, '119' as PREL_SOURCE_ACOMPTE, '228' as DEDUC_CGA)
                )
            ),
            ibica_final AS (
                SELECT NUM_IFU, ANNEE_FISCAL, 
                       NVL(CA_HTVA, 0) as CA_HTVA, 
                       NVL(BENEFICE_IMPOSABLE, 0) as BENEFICE_IMPOSABLE,
                       NVL(IBENEF_EXIGIBLE, 0) as IBENEF_EXIGIBLE, 
                       NVL(IBENEF_DUES, 0) as IBENEF_DUES,
                       NVL(PREL_SOURCE_ACOMPTE, 0) as PREL_SOURCE_ACOMPTE, 
                       NVL(RETENUE_SOURCE_IMPUTE, 0) as RETENUE_SOURCE_IMPUTE
                FROM ibica_pivot
            ),
            is_pivot AS (
                SELECT * FROM (
                    SELECT sid_contribuable.NUM_IFU, 
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
                      AND sid_periode_fiscale.EXERCICE_GESTION > '2022'
                      AND sid_titre_recette.ETAT_ACTUEL != 'ANNULE'
                )
                PIVOT (
                    SUM(VALEUR_NUMERIQUE) 
                    FOR CODE_ELT_LIQ IN ('75' as COTIS_IS_DUE, '73' as IRVM_SUBI, '76' as PREL_SOURCE_ACOMPTE, 
                                          '71' as IBENEF_EXIGIBLE, '77' as RETENUE_SOURCE_IMPUTE, '70' as BENEFICE_IMPOSABLE,
                                          '69' as CA_HTVA, '72' as ACOMPTES_PROV, '74' as IRC_SUBI)
                )
            ),
            is_final AS (
                SELECT NUM_IFU, ANNEE_FISCAL, 
                       NVL(CA_HTVA, 0) as CA_HTVA, 
                       NVL(BENEFICE_IMPOSABLE, 0) as BENEFICE_IMPOSABLE,
                       NVL(IBENEF_EXIGIBLE, 0) as IBENEF_EXIGIBLE, 
                       NVL(COTIS_IS_DUE, 0) as IBENEF_DUES,
                       NVL(PREL_SOURCE_ACOMPTE, 0) as PREL_SOURCE_ACOMPTE, 
                       NVL(RETENUE_SOURCE_IMPUTE, 0) as RETENUE_SOURCE_IMPUTE
                FROM is_pivot
            ),
            bav_combined AS (
                SELECT * FROM ibnc_final
                UNION ALL
                SELECT * FROM ibica_final
                UNION ALL
                SELECT * FROM is_final
            )
            SELECT 
                NUM_IFU, 
                ANNEE_FISCAL,
                SUM(CA_HTVA) as CA_HTVA,
                SUM(BENEFICE_IMPOSABLE) as BENEFICE_IMPOSABLE,
                SUM(IBENEF_EXIGIBLE) as IBENEF_EXIGIBLE,
                SUM(IBENEF_DUES) as IBENEF_DUES,
                SUM(PREL_SOURCE_ACOMPTE) as PREL_SOURCE_ACOMPTE,
                SUM(RETENUE_SOURCE_IMPUTE) as RETENUE_SOURCE_IMPUTE
            FROM bav_combined
            GROUP BY NUM_IFU, ANNEE_FISCAL
            ORDER BY NUM_IFU, ANNEE_FISCAL
            """
# Note: Le script R n'utilise pas de table PROG_DCF.DCF_COMPTABILITE
# Il utilise le fichier Excel BASE_DONNEES.xlsx à la place
# Cette requête a été supprimée pour correspondre exactement au script R

sql_insd = """
SELECT 
     i."Numero_IFU" as NUM_IFU,
    COALESCE(i."ID_INSD", r."ID_INSD", df."ID_INSD", p."ID_INSD", a."ID_INSD", ps."ID_INSD", dfin."ID_INSD", s."ID_INSD") AS ID_INSD,
    i."Annee",
     i."FDP_AcqApCreat",
     i."BLLDS_AcqApCreat", 
     i."BLLDS_VirepostPost",
    r."XA_MargCommerc" as XA_MargCommerc_31_12_N_Net, 
    r."XB_CA" as XB_CA_31_12_N_Net, 
    r."XE_RESULT_EXPL",
    df."EtatTVA_AnneeN",
    p."TOTAL_PImmobilise",
    p."TOTAL_PVenduePays",
    a."AZ_TActifImmob_Brut", 
    a."BI_Clients_Net",
    ps."DN_PRCCT_Net", 
    ps."CP_TCPReA_Net",
    dfin."TPRC_AnneeN",
    s."FR_AnneeN",
    r."RK_ChargDePersonnel" ,
    r."RL_DotAmortProviDep" ,
    r.XH_RESULTAT_HAO as XH_RESULTAT_HAO_31_12_N_Net
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
