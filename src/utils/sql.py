from sqlalchemy.sql import text

from dotenv import load_dotenv


load_dotenv()

sql_to_insert_paiGroupePaiement = text("""
    INSERT INTO paiGroupePaiement (
        date, ncc, idRefNaturePaiement, statutGroupPaiement, montantGroupePaiement, 
        montantConstate, ratioPaiement, raisonSociale, NUMERO_QUITTANCE, dateQuittance
    ) VALUES (
        :date, :ncc, :idRefNaturePaiement, :statutGroupPaiement, :montantGroupePaiement, 
        :montantConstate, :ratioPaiement, :raisonSociale, :NUMERO_QUITTANCE, :dateQuittance
    )
""")
sql_to_get_idPaiGroupePaiement = text("""
    SELECT idPaiGroupePaiement 
    FROM paiGroupePaiement 
    WHERE NUMERO_QUITTANCE = :NUMERO_QUITTANCE 
    AND ncc = :ncc 
    AND montantConstate = :montantConstate
""")

sql_select_str_reglement_impot = text("""
    SELECT 
        eti.ID_DECLARATION,
        stri.date_creation ,
        stri.MONTANT_RGL_IMPT,
        stri.NUMERO_QUITTANCE,
        stri.ID_IMPOT
    FROM 
        S_T_REGLEMENT_IMPOT stri
    INNER JOIN 
        S_T_REGLEMENT str ON str.NUMERO_QUITTANCE = stri.NUMERO_QUITTANCE
    INNER JOIN 
        E_T_IMPOT eti ON stri.ID_IMPOT = eti.ID_IMPOT
    WHERE 
        stri.STATUT_TAMPON = 1
        AND str.STATUT_TAMPON = 1
        AND stri.NUMERO_QUITTANCE = :NUMERO_QUITTANCE
""")

sql_check_if_idDeclaration = text("""
    SELECT idDeclDeclaration 
    FROM declDeclaration 
    WHERE id_declaration IN :id_declaration
""")

sql_update_STREGLEMENTIMPOT = text("""
    UPDATE S_T_REGLEMENT_IMPOT
    SET STATUT_TAMPON = 0
    WHERE NUMERO_QUITTANCE = :NUMERO_QUITTANCE
    AND id_declaration =:id_declaration
""")

sql_update_id_declaration_STREGLEMENTIMPOT = text("""
    UPDATE S_T_REGLEMENT_IMPOT
    SET id_declaration = :id_declaration
    WHERE id_impot = :id_impot
""")

sql_update_montant_restant = text("""
    UPDATE declDeclaration
    SET montantRestant = :montantRestant 
    WHERE idDeclDeclaration = :idDeclDeclaration
""")


sql_to_insert_paiPaiement = text("""
    INSERT INTO paiPaiement (
        idDeclDeclaration, dateCreation, idRefStatutPaiement, montantEffectues, 
        datePaiement, ncc, libellePaiement, idPaiGroupePaiement
    ) VALUES (
        :idDeclDeclaration, :dateCreation, :idRefStatutPaiement, :montantEffectues, 
        :datePaiement, :ncc, :libellePaiement, :idPaiGroupePaiement
    )
""")

sql_update_STATUT_TAMPON_STREGLEMENT = text("""
    UPDATE S_T_REGLEMENT
SET STATUT_TAMPON = 0
WHERE NUMERO_QUITTANCE = :NUMERO_QUITTANCE
""")

sql_to_recupere_montant_restant = text("""
SELECT montantRestant FROM declDeclaration 
WHERE idDeclDeclaration =:idDeclDeclaration
""")
sql_to_verifier_existing_numero_quittance = text("""
                    SELECT NUMERO_QUITTANCE
                    FROM paiGroupePaiement
                    WHERE NUMERO_QUITTANCE = :NUMERO_QUITTANCE
                """)
sql_to_id_decl_et_impot = text(
    """ 
    SELECT id_declaration 
    from e_t_impot 
    WHERE id_impot =:id_impot 
    FETCH FIRST 1 ROWS ONLY
    """
)
sql_to_update_tampo_and_id_decl_srtimpot = text("""
    UPDATE S_T_REGLEMENT_IMPOT
    SET STATUT_TAMPON = 0,id_declaration = :id_declaration
    WHERE NUMERO_QUITTANCE = :NUMERO_QUITTANCE
    AND id_impot =:id_impot
""")
sql_to_verifier_existing_numero_quittance = text("""
                    SELECT NUMERO_QUITTANCE
                    FROM paiGroupePaiement
                    WHERE NUMERO_QUITTANCE = :NUMERO_QUITTANCE
                """)


#########################ods######################### 
Req_DONNEES_DGD=("""SELECT  *  FROM sid_dgd_CPF,SID_CONTRIBUABLE WHERE sid_dgd_CPF.IFU=SID_CONTRIBUABLE.NUM_IFU AND CODE_REG_FISC in ('RN') AND extract(year from date_liquidation)=2025""")

Req_marchexo=("""select * from sid_contrat_acte,sid_contribuable
where  extract(year from date_enregistrement)>2024
AND sid_contrat_acte.ID_contribuable=sid_contribuable.id_contr
""")

reqCONTRATS_NAFOLO=("""select NATURE_PRESTATION,NOM_SECTION,NO_IFU,NOM_FOURNISSEUR,MODE_PASSATION,DATE_APPROBATION,extract(month from DATE_APPROBATION) MOIS, OBJET_CONTRAT,MONTANT_CONTRAT,EXERCICE 
                    from CONTRATS_NAFOLO where EXERCICE=2025;""")

ReqASSUJETIS=("""SELECT NUM_IFU,CODE_NAT_IMP,DATE_PRISE_EN_CPTE 
from ods.sid_impot_contribuable,ods.sid_contribuable 
where ods.sid_impot_contribuable.id_contr=ods.sid_contribuable.id_contr 
and CODE_NAT_IMP in ('IUTS_S','IUTS_T','11','47','48','55','24') """)

Req_marchexo=("""select * from sid_contrat_acte,sid_contribuable
where  extract(year from date_enregistrement)>2024
AND sid_contrat_acte.ID_contribuable=sid_contribuable.id_contr
""")

