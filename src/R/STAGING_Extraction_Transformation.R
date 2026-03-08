library(DBI)
library(odbc)
library(data.table)
library(readxl)
library(dplyr)
#library(janitor)
library(data.table) # Ou RODBC si tu utilises sqlQuery
library(tidyr)
library(stringr)
library(openxlsx)

#setwd("Z:/PV_Q1_2024")
# 🔌 Connexion à Oracle
channel <- dbConnect(odbc::odbc(),
                     Driver = "Oracle in XE",               # nom du driver exact
                     Dbq    = "10.3.1.32:1521/SIDDGI",  # chaîne Easy Connect (host:port/service_name)
                     UID    = "ODSDI1",
                     PWD    = "odsdi1")


##########################DGD##############################################

Req_DGD<-"WITH DGD_PREP AS (
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
"

DGD_IMPORT_EXPORT_AN=dbGetQuery(channel, Req_DGD)

#write.xlsx(DGD_IMPORT_EXPORT_AN,"C:/Users/Administrateur/Desktop/Systeme_Decisionnel/ANARISQUE/Data/DGD_IMPORT_EXPORT_AN.xlsx")

############################ DEUXIEME TRAITEMENT ############################

# 1. Requête SQL optimisée : on sélectionne uniquement les colonnes nécessaires
Req_TVA_DEDUCTION <- "
SELECT NUM_IFU, NOM, ANNEE_FISCAL, MOIS_FISCAL, 
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
"

# 2. Exécution de la requête SQL
DCF_TVA_DECLARATION <- dbGetQuery(channel, Req_TVA_DEDUCTION)

# 3. Nettoyage : remplacer les NA par 0 dans les colonnes numériques uniquement
num_cols <- sapply(DCF_TVA_DECLARATION, is.numeric)
DCF_TVA_DECLARATION[num_cols] <- lapply(DCF_TVA_DECLARATION[num_cols], function(x) ifelse(is.na(x), 0, x))

# 4. Agrégation annuelle avec dplyr
DCF_TVA_DECLARATION_AN <- DCF_TVA_DECLARATION %>%
  group_by(NUM_IFU, NOM, ANNEE_FISCAL) %>%
  summarise(across(
    c(MONTANT_DECLARE, 
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
      CREDIT_TVA_A_REMBOURSER_26),
    ~sum(.x, na.rm = TRUE)
  ), .groups = "drop")

# 5. Extraire la TVA de décembre
DCF_TVA_DECLARATION_AN_12 <- DCF_TVA_DECLARATION %>%
  filter(MOIS_FISCAL == 12) %>%
  group_by(NUM_IFU, ANNEE_FISCAL) %>%
  summarise(TVA_DECEMBRE = sum(MONTANT_TVA_NET_A_PAYER_25, na.rm = TRUE), .groups = "drop")

# 6. Jointure
DCF_TVA_DECLARATION_AN <- DCF_TVA_DECLARATION_AN %>%
  left_join(DCF_TVA_DECLARATION_AN_12, by = c("NUM_IFU", "ANNEE_FISCAL")) %>%
  mutate(TVA_DECEMBRE = replace_na(TVA_DECEMBRE, 0))

# 7. Ajouter colonne ANNEE si besoin
DCF_TVA_DECLARATION_AN$ANNEE <- DCF_TVA_DECLARATION_AN$ANNEE_FISCAL

# (Optionnel) Sauvegarde
# write.csv(DCF_TVA_DECLARATION_AN, "DCF_TVA_DECLARATION_AN.csv", row.names = FALSE)

######################## TVA ANNUEL DEDUITE ###################################

# 1. Requête SQL avec sélection explicite des colonnes utiles
Req_TVA_DEDUCTION <- "
SELECT NUM_IFU_CLIENT, NUM_IFU_FOURN, ANNEE_FISCAL, MOIS_FISCAL,
       TVA_DEDUCTIBLE, TVA_FACTURE, ID_IMPOT, PR_HT, NATURE_DEDUCTION
FROM PROG_DCF.DCF_TVA_FACTURE_DEDUITE
"

# 2. Lecture optimisée
TVA_DEDUCTION_ODS <- dbGetQuery(channel, Req_TVA_DEDUCTION)

# 3. Normalisation IFU Fournisseur
DCF_TVA_DEDUCTION <- TVA_DEDUCTION_ODS %>%
  mutate(
    NUM_IFU_FOURN = str_to_upper(NUM_IFU_FOURN),
    NUM_IFU_FOURN = str_replace_all(NUM_IFU_FOURN, " ", ""),
    NUM_IFU_FOURN = str_replace_all(NUM_IFU_FOURN, "[^[:alnum:]]", ""),
    NUM_IFU_FOURN2 = NUM_IFU_FOURN
  )

# 4. Agrégation NOV-DEC Client
DCF_TVA_DEDUCTION_AN_CLIENT_11_12 <- DCF_TVA_DEDUCTION %>%
  filter(MOIS_FISCAL %in% c(11, 12)) %>%
  group_by(NUM_IFU_CLIENT, ANNEE_FISCAL) %>%
  summarise(
    Cli_TVA_DEDUCTIBLE_NOV_DEC = sum(TVA_DEDUCTIBLE, na.rm = TRUE),
    Cli_PR_HT_NOV_DEC = sum(PR_HT, na.rm = TRUE),
    Cli_TVA_FACTURE_NOV_DEC = sum(TVA_FACTURE, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  rename(NUM_IFU = NUM_IFU_CLIENT, ANNEE = ANNEE_FISCAL)

# 5. Agrégation NOV-DEC Fournisseur
DCF_TVA_DEDUCTION_AN_FOURN_11_12 <- DCF_TVA_DEDUCTION %>%
  filter(MOIS_FISCAL %in% c(11, 12)) %>%
  group_by(NUM_IFU_FOURN, ANNEE_FISCAL) %>%
  summarise(
    Fourn_TVA_DEDUCTIBLE_NOV_DEC = sum(TVA_DEDUCTIBLE, na.rm = TRUE),
    Fourn_PR_HT_NOV_DEC = sum(PR_HT, na.rm = TRUE),
    Fourn_TVA_FACTURE_NOV_DEC = sum(TVA_FACTURE, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  rename(NUM_IFU = NUM_IFU_FOURN, ANNEE = ANNEE_FISCAL)

# 6. Agrégation ANNUELLE Client
DCF_TVA_DEDUCTION_AN_CLIENT <- DCF_TVA_DEDUCTION %>%
  group_by(NUM_IFU_CLIENT, ANNEE_FISCAL) %>%
  summarise(
    Cli_TVA_DEDUCTIBLE_AN = sum(TVA_DEDUCTIBLE, na.rm = TRUE),
    Cli_PR_HT_AN = sum(PR_HT, na.rm = TRUE),
    Cli_TVA_FACTURE_AN = sum(TVA_FACTURE, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  rename(NUM_IFU = NUM_IFU_CLIENT, ANNEE = ANNEE_FISCAL)

# 7. Agrégation ANNUELLE Fournisseur
DCF_TVA_DEDUCTION_AN_FOURN <- DCF_TVA_DEDUCTION %>%
  group_by(NUM_IFU_FOURN, ANNEE_FISCAL) %>%
  summarise(
    Fourn_TVA_DEDUCTIBLE_AN = sum(TVA_DEDUCTIBLE, na.rm = TRUE),
    Fourn_PR_HT_AN = sum(PR_HT, na.rm = TRUE),
    Fourn_TVA_FACTURE_AN = sum(TVA_FACTURE, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  rename(NUM_IFU = NUM_IFU_FOURN, ANNEE = ANNEE_FISCAL)

# 8. Agrégation Importations uniquement
DCF_TVA_DEDUCTION_DGD <- DCF_TVA_DEDUCTION %>%
  filter(NATURE_DEDUCTION %in% c("BAIS Importation", "IMM importation")) %>%
  group_by(NUM_IFU_CLIENT, ANNEE_FISCAL) %>%
  summarise(TVA_SUPPORTE_IMPORT = sum(TVA_DEDUCTIBLE, na.rm = TRUE), .groups = "drop") %>%
  rename(NUM_IFU = NUM_IFU_CLIENT, ANNEE = ANNEE_FISCAL)

# 9. Jointures optimisées
BD_TVA_DEDUC <- DCF_TVA_DEDUCTION_AN_CLIENT_11_12 %>%
  full_join(DCF_TVA_DEDUCTION_AN_FOURN_11_12, by = c("NUM_IFU", "ANNEE")) %>%
  full_join(DCF_TVA_DEDUCTION_AN_CLIENT, by = c("NUM_IFU", "ANNEE")) %>%
  full_join(DCF_TVA_DEDUCTION_AN_FOURN, by = c("NUM_IFU", "ANNEE"))

# 10. Fusion finale avec données de déclaration
BD_TVA <- DCF_TVA_DECLARATION_AN %>%
  left_join(BD_TVA_DEDUC, by = c("NUM_IFU", "ANNEE")) %>%
  left_join(DCF_TVA_DEDUCTION_DGD, by = c("NUM_IFU", "ANNEE")) %>%
  mutate(across(where(is.numeric), ~replace_na(.x, 0)))


########################################DCF_PROG###############

BD_TVA$ANNEE=as.character(BD_TVA$ANNEE)

DCF_PROG1=full_join(DGD_IMPORT_EXPORT_AN,BD_TVA,by=c("NUM_IFU","ANNEE"))
DCF_PROG1[is.na(DCF_PROG1)]=0

#############################################CONTRIBUABLE#############

Req_DCF_IFU="SELECT * FROM SID_CONTRIBUABLE"
DCF_IFU= dbGetQuery(channel,Req_DCF_IFU)



Req_DCF_IFU3="SELECT NUMEROIFU,LIBELLE_DCI FROM SID_CONTRIBUABLE_IMMAT"
DCF_IFU3= dbGetQuery(channel,Req_DCF_IFU3)

#DCF_IFU3=DCF_DCF_IFU3[,c("NUMEROIFU","LIBELLE_DCI")]
names(DCF_IFU3)=c("NUM_IFU","STRUCTURES")

DCF_IFU=left_join(DCF_IFU,DCF_IFU3, by="NUM_IFU")

setwd("Z:/PV_Q1_2024")

DCF_ACTIVITES=read_excel('DCF_ACTIVITES.xlsx')

DCF_ACTIVITES$CODE_SECT_ACT<-as.character(DCF_ACTIVITES$CODE_SECT_ACT)
CONTRIBUABLE=full_join(DCF_IFU,DCF_ACTIVITES,by="CODE_SECT_ACT")


#Traitement du telephone
CONTRIBUABLE$Tel1<- str_replace_all( CONTRIBUABLE$TEL, " ", "")
CONTRIBUABLE$Tel2<- gsub("[^[:alnum:][:space:]]", "", CONTRIBUABLE$Tel1)
CONTRIBUABLE$Tel3<- gsub("[^A-Za-z0-9]]", "", CONTRIBUABLE$Tel2)
CONTRIBUABLE$Tel4=str_extract_all(CONTRIBUABLE$Tel3, pattern ="\\d+" )



DCF_PROG2=full_join(CONTRIBUABLE,DCF_PROG1, by="NUM_IFU")


#DCF_PROG2[is.na(DCF_PROG2)]=0
#BASE_DONNEES <- read_excel("/partagedash/App_Control_Fiscal/BASE_DONNEES.xlsx")

BASE_DONNEES <- read_excel("C:/Users/Administrateur/Desktop/Systeme_Decisionnel/ANARISQUE/Data/BASE_DONNEES/BASE_DONNEES.xlsx")
BASE_DONNEES$NUM_IFU=BASE_DONNEES$Num_IFU_Contribuable
#BASE_DONNEES[is.na(BASE_DONNEES)]=0

BASE_DONNEES$ANNEE=as.character(BASE_DONNEES$ANNEE)
DCF_PROG3=full_join(DCF_PROG2,BASE_DONNEES, by=c("NUM_IFU","ANNEE"))

#PROGRAMMATION<- read_excel("/partagedash/App_Control_Fiscal/DATE_PROG.xlsx")
#PROGRAMMATION <- read_excel("Z:/DATE_PROG.xlsx")

REQ_PROGRAMMATION="SELECT *
FROM
(SELECT  ID_CONTR,TYPE_CONTROLE, max(DATE_PROGR) DATE_DERNIERE_VG
from  SID_PROGRAMME_VERIFICATION 
WHERE TYPE_CONTROLE = 'GENERAL'
GROUP BY ID_CONTR,NUM_IFU,TYPE_CONTROLE) VG
FULL JOIN 

(SELECT  ID_CONTR,TYPE_CONTROLE, max(DATE_PROGR) DATE_DERNIERE_VP
from  SID_PROGRAMME_VERIFICATION 
WHERE TYPE_CONTROLE  ! = 'GENERAL'
GROUP BY ID_CONTR,TYPE_CONTROLE) VP
ON VG.ID_CONTR=VP.ID_CONTR;
"


PROGRAMMATION<- dbGetQuery(channel, REQ_PROGRAMMATION)

#write.csv(PROGRAMMATION,"PROGRAMMATION.csv")


PROGRAMMATION$ID_CONTR=ifelse(is.na(PROGRAMMATION$ID_CONTR),PROGRAMMATION[,4],PROGRAMMATION$ID_CONTR)

Req_ID_CONTR_IFU="select distinct ID_CONTR,NUM_IFU,DATE_IMMAT from SID_CONTRIBUABLE;"
ID_CONTR_IFU<- dbGetQuery(channel, Req_ID_CONTR_IFU)


#write.csv(ID_CONTR_IFU,"ID_CONTR_IFU.csv")

## Djamila
PROGRAMMATION <- PROGRAMMATION[, !duplicated(names(PROGRAMMATION))]

PROGRAMMATION=left_join(PROGRAMMATION,ID_CONTR_IFU, by="ID_CONTR")

PROGRAMMATION=PROGRAMMATION[,c("ID_CONTR","NUM_IFU","DATE_IMMAT","DATE_DERNIERE_VG","DATE_DERNIERE_VP") ]


Req_AVIS="SELECT DISTINCT T2.ID_CONTR,T4.NUM_IFU,T4.NOM_MINEFID,DATE_DERNIERE_AVIS,DERNIERE_GESTION_SOUMIS_VERIF,NOMBRE_EXERC_SOUMIS_VERIF FROM 

(SELECT 
ID_CONTR, 
MAX(SID_CORRESPONDANCE_BRIGADE.DATE_CORRESP) DATE_DERNIERE_AVIS, MAX(CODE_GEST) DERNIERE_GESTION_SOUMIS_VERIF,COUNT(CODE_GEST) NOMBRE_EXERC_SOUMIS_VERIF
FROM SID_CORRESPONDANCE_BRIGADE
WHERE CODE_TYP_COR=4
GROUP BY ID_CONTR) T2

LEFT JOIN

(SELECT ID_CONTR, NUM_IFU,NOM_MINEFID from SID_CONTRIBUABLE ) T4

ON T2.ID_CONTR=T4.ID_CONTR;"


DATE_AVIS<- dbGetQuery(channel, Req_AVIS)

#write.csv(DATE_AVIS,"DATE_AVIS.csv")



DATE_AVIS=DATE_AVIS[!duplicated(DATE_AVIS$ID_CONTR),]

names(DATE_AVIS)
PROGRAMMATION_AVIS=full_join(PROGRAMMATION,DATE_AVIS,by="ID_CONTR")

PROGRAMMATIONS=PROGRAMMATION_AVIS[,c("NUM_IFU.x", "DATE_IMMAT","DATE_DERNIERE_VG","DATE_DERNIERE_VP","NOM_MINEFID","DATE_DERNIERE_AVIS","DERNIERE_GESTION_SOUMIS_VERIF")]

names(PROGRAMMATIONS)=c("NUM_IFU", "DATE_IMMAT","DATE_DERNIERE_VG","DATE_DERNIERE_VP","NOM_MINEFID","DATE_DERNIERE_AVIS","DERNIERE_GESTION_SOUMIS_VERIF")

#View(subset(PROGRAMMATION_AVIS,is.na(PROGRAMMATION_AVIS$DATE_IMMAT)))
PROGRAMMATIONS[order(c(PROGRAMMATIONS$DATE_DERNIERE_VG,PROGRAMMATIONS$DATE_DERNIERE_VP), decreasing = T),]


PROGRAMMATIONS=PROGRAMMATIONS[!duplicated(PROGRAMMATIONS$NUM_IFU),]
length(unique(PROGRAMMATIONS$NUM_IFU))

# View(subset(PROGRAMMATIONS,PROGRAMMATIONS$NUM_IFU=='00016079H'))


DCF_PROG3=left_join(DCF_PROG3,PROGRAMMATIONS, by=c("NUM_IFU","NOM_MINEFID","DATE_IMMAT"))


#BENEFICE
#BENEFICE
Req_IBNC="select * from (
   Select sid_contribuable.num_IFU, extract(year from DATE_FIN_PERIODE) ANNEE_FISCAL,sid_element_liquidation.code_elt_liq, sid_det_ele_liquidations.valeur_numerique
from sid_element_liquidation, sid_det_ele_liquidations,
sid_contribuable, sid_titre_recette, sid_impot, sid_periode_fiscale
where sid_det_ele_liquidations.code_elt_liq= sid_element_liquidation.code_elt_liq
and sid_det_ele_liquidations.id_impot=sid_impot.id_impot
and sid_impot.code_titre=sid_titre_recette.code_titre
and sid_titre_recette.id_contr=sid_contribuable.id_contr
--and sid_element_liquidation.code_elt_liq='95'
and sid_det_ele_liquidations.code_nat_imp in ('27')
and sid_impot.id_cal_fisc= sid_periode_fiscale.code_cal_fisc
and sid_periode_fiscale.exercice_gestion>'2022'
and sid_titre_recette.etat_actuel !='ANNULE'

)
pivot 
(
   sum(valeur_numerique) for code_elt_liq in('69','120','115','119','122','123','228')   
)
order by num_IFU ;
"
IBNC=dbGetQuery(channel, Req_IBNC)

#write.csv(IBNC,"IBNC.csv")

names(IBNC)=c("NUM_IFU","ANNEE_FISCAL","Chiffre d'affaires hors TVA","Retenues ? la source subies imput?s : (d?claration des retenues subies et attestatios des retenues ci-jointes, Montant T4)","B?n?fice imposable", "Cotisation BNC exigible ( ou minimum de perception)","cotisation BNC ? payer (02 - (03 + 04))*","Montant de la d?duction adh?rent CGA")

IBNC2=IBNC[,c("NUM_IFU","ANNEE_FISCAL","Chiffre d'affaires hors TVA", "B?n?fice imposable","Cotisation BNC exigible ( ou minimum de perception)","cotisation BNC ? payer (02 - (03 + 04))*","Montant de la d?duction adh?rent CGA","Retenues ? la source subies imput?s : (d?claration des retenues subies et attestatios des retenues ci-jointes, Montant T4)")]

names(IBNC2)=c("NUM_IFU","ANNEE_FISCAL","CA_HTVA", "BENEFICE_IMPOSABLE","IBENEF_EXIGIBLE","IBENEF_DUES","PREL_SOURCE_ACOMPTE","RETENUE_SOURCE_IMPUTE")


Req_IBICA="select * from (
   Select sid_contribuable.num_IFU, extract(year from DATE_FIN_PERIODE) ANNEE_FISCAL,sid_element_liquidation.code_elt_liq, sid_det_ele_liquidations.valeur_numerique
from sid_element_liquidation, sid_det_ele_liquidations,
sid_contribuable, sid_titre_recette, sid_impot, sid_periode_fiscale
where sid_det_ele_liquidations.code_elt_liq= sid_element_liquidation.code_elt_liq
and sid_det_ele_liquidations.id_impot=sid_impot.id_impot
and sid_impot.code_titre=sid_titre_recette.code_titre
and sid_titre_recette.id_contr=sid_contribuable.id_contr
--and sid_element_liquidation.code_elt_liq='95'
and sid_det_ele_liquidations.code_nat_imp in ('03')
and sid_impot.id_cal_fisc= sid_periode_fiscale.code_cal_fisc
and sid_periode_fiscale.exercice_gestion>'2022'
and sid_titre_recette.etat_actuel !='ANNULE'

)
pivot 
(
   sum(valeur_numerique) for code_elt_liq in('69','121','116','118','120','115','117','119','228')   
)
order by num_IFU ;"
IBICA=dbGetQuery(channel, Req_IBICA)

#write.csv(IBICA,"IBICA.csv")

names(IBICA)=c("NUM_IFU","ANNEE_FISCAL","Chiffre d'affaires hors TVA","Cotisation BIC a payer (04 - (05 + 06)*","Cotisation BIC exigible ( ou MFP)","Cotisation BIC due (02-03)","Retenues ? la source subies imput?s : (d?claration des retenues subies et attestatios des retenues ci-jointes, Montant T4)","B?n?fice imposable","Cotisation MFP d?clar?es","Pr?l?vement ? la source ? titre d'acompte sur les impots sur les b?n?fices imput?s ( d?claration des pr?l?vements support?s ci annex?e, montant IV)","Montant de la d?duction adh?rent CGA")

IBICA2=IBICA[,c("NUM_IFU","ANNEE_FISCAL","Chiffre d'affaires hors TVA", "B?n?fice imposable","Cotisation BIC exigible ( ou MFP)","Cotisation BIC due (02-03)","Pr?l?vement ? la source ? titre d'acompte sur les impots sur les b?n?fices imput?s ( d?claration des pr?l?vements support?s ci annex?e, montant IV)","Retenues ? la source subies imput?s : (d?claration des retenues subies et attestatios des retenues ci-jointes, Montant T4)")]

names(IBICA2)=c("NUM_IFU","ANNEE_FISCAL","CA_HTVA", "BENEFICE_IMPOSABLE","IBENEF_EXIGIBLE","IBENEF_DUES","PREL_SOURCE_ACOMPTE","RETENUE_SOURCE_IMPUTE")

Req_IS="select * from (
   Select sid_contribuable.num_IFU, extract(year from DATE_FIN_PERIODE) ANNEE_FISCAL,sid_element_liquidation.code_elt_liq, sid_det_ele_liquidations.valeur_numerique
from sid_element_liquidation, sid_det_ele_liquidations,
sid_contribuable, sid_titre_recette, sid_impot, sid_periode_fiscale
where sid_det_ele_liquidations.code_elt_liq= sid_element_liquidation.code_elt_liq
and sid_det_ele_liquidations.id_impot=sid_impot.id_impot
and sid_impot.code_titre=sid_titre_recette.code_titre
and sid_titre_recette.id_contr=sid_contribuable.id_contr
--and sid_element_liquidation.code_elt_liq='95'
and sid_det_ele_liquidations.code_nat_imp in ('88','111')
and sid_impot.id_cal_fisc= sid_periode_fiscale.code_cal_fisc
and sid_periode_fiscale.exercice_gestion>'2022'
and sid_titre_recette.etat_actuel !='ANNULE'

)
pivot 
(
   sum(valeur_numerique) for code_elt_liq in('75','73','76','71','77','70','69','72','74')   
)
order by num_IFU ;"

IS=dbGetQuery(channel, Req_IS)

#write.csv(IS,"IS.csv")

names(IS)=c("NUM_IFU","ANNEE_FISCAL","Cotisation IS due","IRVM subi (attestation jointe)","Pr?l?vements ? la source ? titre d'acompte d'impot sur les b?n?fices imput?s","Cotisation IS exigible (ou Minimum forfaitaire de perception)","Retenues ? la source subies imput?es","R?sultat fiscal","Chiffre d'affaires hors TVA","Acomptes provisionnels d?clar?","IRC subi (attestation jointe)")


IS2=IS[,c("NUM_IFU","ANNEE_FISCAL",
          "Chiffre d'affaires hors TVA",
          "R?sultat fiscal",
          "Cotisation IS exigible (ou Minimum forfaitaire de perception)",
          "Cotisation IS due",
          "Pr?l?vements ? la source ? titre d'acompte d'impot sur les b?n?fices imput?s",
          "Retenues ? la source subies imput?es")]
names(IS2)=c("NUM_IFU","ANNEE_FISCAL","CA_HTVA", "BENEFICE_IMPOSABLE","IBENEF_EXIGIBLE","IBENEF_DUES","PREL_SOURCE_ACOMPTE","RETENUE_SOURCE_IMPUTE")

BAV=rbind(IBNC2,IBICA2,IS2)  
BAV[is.na(BAV)]=0
BAV$ANNEE_FISCAL=as.numeric(BAV$ANNEE_FISCAL)
BAV=unique(BAV)


#BAV_bis=BAV[!duplicated(BAV$ANNEE_FISCAL,BAV$NUM_IFU),]
BAV_bis = BAV[!duplicated(BAV[, c("ANNEE_FISCAL", "NUM_IFU")]), ]
BAV_agg=aggregate(cbind(CA_HTVA, BENEFICE_IMPOSABLE,IBENEF_EXIGIBLE,IBENEF_DUES,PREL_SOURCE_ACOMPTE,RETENUE_SOURCE_IMPUTE)~NUM_IFU+ANNEE_FISCAL,BAV,sum)

DCF_PROG3V0=left_join(DCF_PROG3,BAV_agg,by=c("NUM_IFU","ANNEE_FISCAL"))


DCF_PROG3V0=(subset(DCF_PROG3V0,DCF_PROG3V0$ETAT=="ACTIF"))

Entreprises_LIEES <- read_excel("Z:/App_Control_Fiscal/Entreprises_LIEES.xlsx")

CLIENTS=Entreprises_LIEES
names(CLIENTS)=c("NUM_IFU_CLIENT","TELEPHONE","Groupe_IFU_cli","Occurrences_IFU" )


FOURNISSEURS=Entreprises_LIEES
names(FOURNISSEURS)=c("NUM_IFU_FOURN","TELEPHONE","Groupe_IFU_fourn","Occurrences_IFU" )

TVA_DEDUCTION_ODS=left_join(TVA_DEDUCTION_ODS,CLIENTS,by=c("NUM_IFU_CLIENT"))
TVA_DEDUCTION_ODS=left_join(TVA_DEDUCTION_ODS,FOURNISSEURS,by=c("NUM_IFU_FOURN"))

TVA_DEDUCTION_ODS$RISQUE_IND_17=ifelse((TVA_DEDUCTION_ODS$Groupe_IFU_cli==TVA_DEDUCTION_ODS$Groupe_IFU_fourn)&(TVA_DEDUCTION_ODS$NUM_IFU_FOURN!=TVA_DEDUCTION_ODS$NUM_IFU_CLIENT),"rouge","vert")

RISQUEDEDUC=(subset(TVA_DEDUCTION_ODS,TVA_DEDUCTION_ODS$RISQUE_IND_17=="rouge"))

RISQUEDEDUC=RISQUEDEDUC[order(RISQUEDEDUC$TVA_DEDUCTIBLE,decreasing = T),]

RISQUEDEDUC2=RISQUEDEDUC[,c("ANNEE_FISCAL","NUM_IFU_FOURN","NUM_IFU_CLIENT","Groupe_IFU_cli","Occurrences_IFU.x","TELEPHONE.x")]
RISQUEDEDUC2=unique(RISQUEDEDUC2)

RISQUEDEDUC2$IFUANNEE_cli=paste0(RISQUEDEDUC2$NUM_IFU_CLIENT,RISQUEDEDUC2$ANNEE_FISCAL)
RISQUEDEDUC2$IFUANNEE_Fourn=paste0(RISQUEDEDUC2$NUM_IFU_FOURN,RISQUEDEDUC2$ANNEE_FISCAL)


IFU_CLI_RISQ=unique(RISQUEDEDUC2$IFUANNEE_cli)
IFU_FOURN_RISQ=unique(RISQUEDEDUC2$IFUANNEE_Fourn)

DCF_PROG3V0=left_join(DCF_PROG3V0,Entreprises_LIEES,by=c("NUM_IFU"))


DCF_PROG<-DCF_PROG3V0

DCF_PROG_1=subset(DCF_PROG,is.na(DCF_PROG$DATE_DERNIERE_VP))
DCF_PROG_1$DATE_DERNIERE_VP=DCF_PROG_1$DATE_IMMAT
DCF_PROG_2=subset(DCF_PROG,(!is.na(DCF_PROG$DATE_DERNIERE_VP)))
DCF_PROG=rbind(DCF_PROG_2,DCF_PROG_1)





DCF_PROG_1=subset(DCF_PROG,is.na(DCF_PROG$DATE_DERNIERE_VG))
DCF_PROG_1$DATE_DERNIERE_VG=DCF_PROG_1$DATE_IMMAT
DCF_PROG_2=subset(DCF_PROG,(!is.na(DCF_PROG$DATE_DERNIERE_VG)))
DCF_PROG=rbind(DCF_PROG_2,DCF_PROG_1)


DCF_PROG_1=subset(DCF_PROG,is.na(DCF_PROG$DATE_DERNIERE_AVIS))
DCF_PROG_1$DATE_DERNIERE_AVIS=DCF_PROG_1$DATE_IMMAT
DCF_PROG_2=subset(DCF_PROG,(!is.na(DCF_PROG$DATE_DERNIERE_AVIS)))
DCF_PROG=rbind(DCF_PROG_2,DCF_PROG_1)


DCF_PROG_RNI=subset(DCF_PROG,DCF_PROG$CODE_REG_FISC %in% c("RN"))

DCF_PROG_RSI=subset(DCF_PROG,DCF_PROG$CODE_REG_FISC %in% c("RSI"))
#fwrite(DCF_PROG_RSI, file ="DCF_PROG_RSIV4.csv")

#write.csv(DCF_PROG_RSI, file ="DCF_PROG_RSI.csv")


DCF_PROG_ND=subset(DCF_PROG,DCF_PROG$CODE_REG_FISC %in% c("ND"))
#fwrite(DCF_PROG_ND, file ="DCF_PROG_ND4.csv")

#write.csv(DCF_PROG_ND, file ="DCF_PROG_ND4.csv")

DCF_PROG_CME=subset(DCF_PROG,DCF_PROG$CODE_REG_FISC %in% c("CME","CME_RD","CSI"))
#fwrite(DCF_PROG_CME, file ="DCF_PROG_CME4.csv")

#write.csv(DCF_PROG_CME, file ="DCF_PROG_CME4.csv")

DCF_PROG_csb=subset(DCF_PROG,DCF_PROG$CODE_REG_FISC %in% c("CSB"))
#write.csv(DCF_PROG_csb, file ="DCF_PROG_csb.csv")

