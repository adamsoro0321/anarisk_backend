library(stringr)
library(gsubfn)
library(readxl)
library(dplyr)
library(purrr)
library(odbc)
library(RPostgreSQL)
library(RPostgres)
library(lubridate)
library(RODBC)
library(reshape2)
library(data.table)
library(readr)
library(openxlsx)


############## Importation des bases pour  calcul ###############################################


# DCF_PROG_CME4<- read_csv("DCF_PROG_CME4.csv")
# 
# DCF_PROG_ND4<- read_csv("DCF_PROG_ND4.csv")
# 
#  DCF_PROG_RNIV4<- read_csv("DCF_PROG_RNIV4.csv")
# 
#  DCF_PROG_RSIV4<- read_csv("DCF_PROG_RSIV4.csv")

# DCF_PROG_total=rbind(DCF_PROG_CME4,DCF_PROG_ND4,DCF_PROG_RNIV4,DCF_PROG_RSIV4)


# ouaga3=subset(DCF_PROG_total, DCF_PROG_total$NUM_IFU %in% c('00050046C','00110451J','00046569X'))




################ Importation de la liste arbitrée Q2 ########################################################


#Q2 <- read_excel("Z:/PV_Q1_2024/Q2.xlsx")
# Q1 <- read_excel("Z:/PV_Q1_2024/Q1.xlsx")
# listeQ1=unique(Q1$NUM_IFU)
# listeQ2=unique(Q2$NUM_IFU)
#1.
#DCF_PROG_total=DCF_PROG_RSI
#2.
DCF_PROG_total=DCF_PROG_ND
#3.
#DCF_PROG_total=DCF_PROG_RSI 
#4.
#DCF_PROG_total=DCF_PROG_CME

#DCF_PROG_total=DCF_PROG

NON_EIGIBLE <- read_excel("NON_ELIGIBLE.xlsx")
LISTE_NON_EIGIBLE=NON_EIGIBLE$NUM_IFU
#View(NON_EIGIBLE)
DCF_PROG=subset(DCF_PROG_total, !(DCF_PROG_total$NUM_IFU %in% LISTE_NON_EIGIBLE))

BD_TVA_Shiny<-DCF_PROG[,c("NUM_IFU","NOM_MINEFID","ETAT","CODE_SECT_ACT","CODE_REG_FISC", "STRUCTURES" ,"ANNEE_FISCAL","DATE_DERNIERE_VG","DATE_DERNIERE_VP","DATE_DERNIERE_AVIS")]

############## INDICATEUR NUMERO 15_A 


# Identifier les entreprises avec une date de programmation et une date d'avis de vérification supérieures à 3 ans
BD_TVA_Shiny$RISQUE_IND_15_A <- ifelse(((BD_TVA_Shiny$DATE_DERNIERE_VG <'2022-12-31') & (BD_TVA_Shiny$DATE_DERNIERE_AVIS <'2022-12-31')), "rouge", "vert")

BD_TVA_Shiny$RISQUE_IND_15_B <- ifelse((BD_TVA_Shiny$DATE_DERNIERE_VP <'2022-12-31') & (BD_TVA_Shiny$DATE_DERNIERE_AVIS <'2022-12-31'), "rouge", "vert")

BD_TVA_Shiny2=subset(BD_TVA_Shiny,(BD_TVA_Shiny$RISQUE_IND_15_A=='rouge')|(BD_TVA_Shiny$RISQUE_IND_15_B=='rouge'))

length(unique(BD_TVA_Shiny$NUM_IFU))

############## INDICATEUR NUMERO 16 
listeNonEl=unique(BD_TVA_Shiny2$NUM_IFU)  
# 
# BD_TVA_ShinyRsiRni=subset(BD_TVA_Shiny,BD_TVA_Shiny$CODE_REG_FISC %in% c("RN","RSI","ND" ))
# fwrite(BD_TVA_ShinyRsiRni,"BD_TVA_ShinyRsiRni.csv")
# 
# BD_TVA_ShinyAUTRE=subset(BD_TVA_Shiny,BD_TVA_Shiny$CODE_REG_FISC %in% c("CME_RD","CME" , NA,"CSI","CSB" ))
# fwrite(BD_TVA_ShinyAUTRE,"BD

# Fonction pour le calcul DU RISQUE DE DELIVRANCE DE FAUSSE FACTURE



#==============================================================================================================================
#============================== CALCUL DE L'INDICATEUR 1 =====================================================================


TVA_IND1 <- function(criticite, numerateur, denominateur, seuil, coeff, x1, x2, x3, x4) {
  
  # Initialisation avec des valeurs par défaut NA
  ecart <- NA_real_
  groupe <- NA_character_
  score <- NA_real_
  
  # Vérification des données d'entrée
  if (is.na(numerateur) | is.na(denominateur)) {
    return(list(ecart = NA_real_, groupe = "Données manquantes", score = NA_real_))
  }
  
  # Cas spécial : dénominateur nul
  if (denominateur == 0) {
    return(list(
      ecart = NA_real_, 
      groupe = "Ratio non calculable", 
      score = NA_real_
    ))
  }
  
  # Calcul de l'indicateur
  indicateur <- numerateur / denominateur
  
  # Comparaison avec le seuil
  if (seuil<indicateur) {
    groupe <- "vert"
    ecart <- 0
    score <- 0
  } else {
    # Calcul de l'écart pondéré
    ecart <- abs((seuil * denominateur) - numerateur) * coeff
    
    # Détermination de l'impact
    impact <- dplyr::case_when(
      ecart < x1 ~ 1,
      ecart < x2 ~ 2,
      ecart < x3 ~ 3,
      ecart < x4 ~ 4,
      TRUE ~ 5
    )
    
    # Calcul du score
    score <- criticite * impact
    
    # Détermination du groupe de risque
    groupe <- dplyr::case_when(
      score <= 4 ~ "vert",
      score %in% c(5, 6) & criticite %in% c(1, 2) ~ "vert",
      score %in% c(5, 6) ~ "jaune",
      score %in% c(8, 9) ~ "jaune",
      score == 10 & criticite == 2 ~ "jaune",
      score == 10 ~ "rouge",
      score %in% c(12, 16) ~ "orange",
      score == 15 & criticite == 3 ~ "orange",
      score == 15 ~ "rouge",
      score >= 20 ~ "rouge",
      TRUE ~ "indéterminé"
    )
  }
  
  # Retour sous forme de liste nommée (plus lisible que vecteur)
  return(list(ecart = ecart, groupe = groupe, score = score))
}

#===================== Appliquer la fonction TVA_IND1() à chaque ligne du dataframe (VERSION VECTORISÉE)

# Constantes pour l'indicateur 1
CRITICITE_IND1 <- 5
SEUIL_IND1 <- 0.2
COEFF_IND1 <- 0.8
SEUILS_IMPACT_IND1 <- list(x1 = 500000, x2 = 5000000, x3 = 20000000, x4 = 100000000)

# Application vectorisée avec purrr::map2
resultats_ind1 <- purrr::map2(
  DCF_PROG$MONTANT_TVA_NET_A_PAYER_25,
  DCF_PROG$Fourn_TVA_DEDUCTIBLE_AN,
  ~ TVA_IND1(
    criticite = CRITICITE_IND1,
    numerateur = .x,
    denominateur = .y,
    seuil = SEUIL_IND1,
    coeff = COEFF_IND1,
    x1 = SEUILS_IMPACT_IND1$x1,
    x2 = SEUILS_IMPACT_IND1$x2,
    x3 = SEUILS_IMPACT_IND1$x3,
    x4 = SEUILS_IMPACT_IND1$x4
  )
)

# Extraire les résultats dans des colonnes séparées
BD_TVA_Shiny$RISQUE_IND_1 <- purrr::map_chr(resultats_ind1, "groupe")
BD_TVA_Shiny$GAP_IND_1 <- purrr::map_dbl(resultats_ind1, "ecart", .default = 0)
BD_TVA_Shiny$SCORE_IND_1 <- purrr::map_dbl(resultats_ind1, "score", .default = 0)

############## INDICATEUR NUMERO 2 
#Declaration de la fonction
TVA_IND2<-function(criticite,numerateur,denominateur,seuil,coeff,x1,x2,x3,x4){
  if(denominateur!=0 & !is.na(denominateur) & !is.na(numerateur)){
    indicateur<-(numerateur/denominateur)
  }
  else{
    indicateur<-0
  }
  if(indicateur<seuil){
    groupe="vert"
    ecart=0
    score=0
    RISQUE<-c(ecart,groupe,score)
  }
  else{
    ecart<-abs(numerateur-(seuil*denominateur))
    ecart=ecart*coeff
    if(ecart<x1){
      impact=1
    }
    else if(ecart<x2){
      impact=2
    }
    else if(ecart<x3){
      impact<-3
    }
    else if(ecart<x4){
      impact<-4
    }
    else{
      impact<-5
    }
    
    score<-criticite*impact
    groupe=switch (as.character(score),
                   '1' = 'vert',
                   '2' = 'vert',
                   '3' = 'vert',
                   '4' = 'vert',
                   '5' = ifelse(as.character(criticite)=="1","vert","jaune"),
                   '6' = ifelse(as.character(criticite)=="2","vert","jaune"),
                   '8' = 'jaune',
                   '9' = 'jaune',
                   '10' = ifelse(as.character(criticite)=="2","jaune","rouge"),
                   '12' = 'orange',
                   '15' = ifelse(as.character(criticite)=="3","orange","rouge"),
                   '16' = 'orange',
                   '20' = 'rouge',
                   '25' = 'rouge'
                   
    )
    RISQUE=c(ecart,groupe,score)
  }
  return(RISQUE)
}

#CALCUL DE L'INDICATEUR DU RISQUE DE FAUSSE FACTURE, IND_1
for (i in 1:nrow(DCF_PROG)){
  if(!is.na(DCF_PROG$Cli_TVA_DEDUCTIBLE_NOV_DEC[i]) & !is.na(DCF_PROG$Cli_TVA_DEDUCTIBLE_AN[i])){
    BD_TVA_Shiny$RISQUE_IND_2[i]=TVA_IND2(4,DCF_PROG$Cli_TVA_DEDUCTIBLE_NOV_DEC[i],DCF_PROG$Cli_TVA_DEDUCTIBLE_AN[i],0.5,0.75,500000,5000000,20000000,100000000)[2]
    BD_TVA_Shiny$GAP_IND_2[i]=TVA_IND2(4,DCF_PROG$Cli_TVA_DEDUCTIBLE_NOV_DEC[i],DCF_PROG$Cli_TVA_DEDUCTIBLE_AN[i],0.5,0.75,500000,5000000,20000000,100000000)[1]
    BD_TVA_Shiny$SCORE_IND_2[i]=TVA_IND2(4,DCF_PROG$Cli_TVA_DEDUCTIBLE_NOV_DEC[i],DCF_PROG$Cli_TVA_DEDUCTIBLE_AN[i],0.5,0.75,500000,5000000,20000000,100000000)[3]
    
    
  }
  else{
    BD_TVA_Shiny$RISQUE_IND_2[i]="Non disponible"
    BD_TVA_Shiny$GAP_IND_2[i]=0
    BD_TVA_Shiny$SCORE_IND_2[i]=0
  }
}

########### INDICATEUR NUMERO 3 

DCF_PROG$duree_en_mois=as.numeric(interval(DCF_PROG$DATE_IMMAT, Sys.Date()) / ddays(30))

TVA_IND3<-function(dateCreation,importation){
  # DÂ©finir la date actuelle
  date_actuelle <- Sys.Date()
  duree_en_mois <- as.numeric(interval(dateCreation, date_actuelle) / ddays(30))
  if (duree_en_mois <= 12 & importation >= 100000000) {
    Risque<- "rouge"
  } else {
    Risque<- "vert"
  }
  Age_en_mois=duree_en_mois
  RISQUE<-c(Risque,Age_en_mois)
  
  return(RISQUE) 
}

for (i in 1:nrow(DCF_PROG)) {
  if(!is.na(DCF_PROG$DATE_IMMAT[i]) & !is.na(DCF_PROG$IMPORT_CAF[i])){
    BD_TVA_Shiny$RISQUE_IND_3[i]=TVA_IND3(DCF_PROG$DATE_IMMAT[i],DCF_PROG$IMPORT_CAF[i])[1]
    
  }
  
  else{
    BD_TVA_Shiny$RISQUE_IND_3[i]="Non disponible"
  }
}

############## INDICATEUR NUMERO 4 
#Declaration de la fonction
IND4<-function(criticite,numerateur,regime,seuil,coeff,x1,x2,x3,x4){
  if(!is.na(numerateur) & !is.na(regime) & regime=="CME"){
    denominateur<-15000000
    indicateur<-(numerateur/denominateur)
  }
  else if(!is.na(numerateur) & !is.na(regime) & regime=="RSI"){
    denominateur<-50000000
    indicateur<-(numerateur/denominateur)
  }
  else{
    indicateur<-0
  }
  
  if(indicateur<seuil){
    groupe="vert"
    ecart=0
    score=0
    RISQUE<-c(ecart,groupe,score)
  }
  else{
    ecart<-abs(numerateur-denominateur)
    ecart=ecart*coeff
    if(ecart<x1){
      impact=1
    }
    else if(ecart<x2){
      impact=2
    }
    else if(ecart<x3){
      impact<-3
    }
    else if(ecart<x4){
      impact<-4
    }
    else{
      impact<-5
    }
    
    score<-criticite*impact
    groupe=switch (as.character(score),
                   '1' = 'vert',
                   '2' = 'vert',
                   '3' = 'vert',
                   '4' = 'vert',
                   '5' = ifelse(as.character(criticite)=="1","vert","jaune"),
                   '6' = ifelse(as.character(criticite)=="2","vert","jaune"),
                   '8' = 'jaune',
                   '9' = 'jaune',
                   '10' = ifelse(as.character(criticite)=="2","jaune","rouge"),
                   '12' = 'orange',
                   '15' = ifelse(as.character(criticite)=="3","orange","rouge"),
                   '16' = 'orange',
                   '20' = 'rouge',
                   '25' = 'rouge'
                   
    )
    RISQUE=c(ecart,groupe,score)
  }
  return(RISQUE)
}

for (i in 1:nrow(DCF_PROG)){
  if(!is.na(DCF_PROG$IMPORT_CAF[i]) & !is.na(DCF_PROG$CODE_REG_FISC[i])){
    BD_TVA_Shiny$RISQUE_IND_4[i]=IND4(5,DCF_PROG$IMPORT_CAF[i],DCF_PROG$CODE_REG_FISC[i],1,0.8,500000,5000000,20000000,100000000)[2]
    BD_TVA_Shiny$GAP_IND_4[i]=IND4(5,DCF_PROG$IMPORT_CAF[i],DCF_PROG$CODE_REG_FISC[i],1,0.8,500000,5000000,20000000,100000000)[1]
    BD_TVA_Shiny$SCORE_IND_4[i]=IND4(5,DCF_PROG$IMPORT_CAF[i],DCF_PROG$CODE_REG_FISC[i],1,0.8,500000,5000000,20000000,100000000)[3]
  }else{
    BD_TVA_Shiny$RISQUE_IND_4[i]="Non disponible"
    BD_TVA_Shiny$GAP_IND_4[i]=0
    BD_TVA_Shiny$SCORE_IND_4[i]=0
  }
}

################# INDICATEUR 5
# MONTANT DES EXPORTATIONS DE L'EXERCICE
DCF_PROG <- DCF_PROG %>%
  mutate(MNT_EXPORTATION_DECLARE = OP_NTAXBLE_EXPORTATIONS_09+OP_NTAXBLE_AUTRES_COMM_EXTERIEUR_VTE_SUSPENSION_TAXE_10)
#Declaration de la fonction
IND5<-function(criticite,numerateur,denominateur,seuil,coeff,x1,x2,x3,x4){
  if(denominateur!=0 & !is.na(denominateur) & !is.na(numerateur)){
    indicateur<-(numerateur/denominateur)
  }
  else{
    indicateur<-0
  }
  
  if(indicateur==seuil){
    groupe="vert"
    ecart=0
    score=0
    RISQUE<-c(ecart,groupe,score)
  }
  else{
    ecart<-abs(numerateur-denominateur)
    ecart=ecart*coeff
    if(ecart<x1){
      impact=1
    }
    else if(ecart<x2){
      impact=2
    }
    else if(ecart<x3){
      impact<-3
    }
    else if(ecart<x4){
      impact<-4
    }
    else{
      impact<-5
    }
    
    score<-criticite*impact
    groupe=switch (as.character(score),
                   '1' = 'vert',
                   '2' = 'vert',
                   '3' = 'vert',
                   '4' = 'vert',
                   '5' = ifelse(as.character(criticite)=="1","vert","jaune"),
                   '6' = ifelse(as.character(criticite)=="2","vert","jaune"),
                   '8' = 'jaune',
                   '9' = 'jaune',
                   '10' = ifelse(as.character(criticite)=="2","jaune","rouge"),
                   '12' = 'orange',
                   '15' = ifelse(as.character(criticite)=="3","orange","rouge"),
                   '16' = 'orange',
                   '20' = 'rouge',
                   '25' = 'rouge'
                   
    )
    RISQUE=c(ecart,groupe,score)
  }
  return(RISQUE)
}

for (i in 1:nrow(DCF_PROG)){
  if(!is.na(DCF_PROG$MNT_EXPORTATION_DECLARE[i]) & !is.na(DCF_PROG$EXPORT_CAF[i])){
    BD_TVA_Shiny$RISQUE_IND_5[i]=IND5(3,DCF_PROG$MNT_EXPORTATION_DECLARE[i],DCF_PROG$EXPORT_CAF[i],1,0.5,500000,5000000,20000000,100000000)[2]
    BD_TVA_Shiny$GAP_IND_5[i]=IND5(3,DCF_PROG$MNT_EXPORTATION_DECLARE[i],DCF_PROG$EXPORT_CAF[i],1,0.5,500000,5000000,20000000,100000000)[1]
    BD_TVA_Shiny$SCORE_IND_5[i]=IND5(3,DCF_PROG$MNT_EXPORTATION_DECLARE[i],DCF_PROG$EXPORT_CAF[i],1,0.5,500000,5000000,20000000,100000000)[3]
  }else{
    BD_TVA_Shiny$RISQUE_IND_5[i]="Non disponible"
    BD_TVA_Shiny$GAP_IND_5[i]=0
    BD_TVA_Shiny$SCORE_IND_5[i]=0
  }
}
################ INDICATEUR 6 
# Calcul de l'acquisition d'immobilisation
DCF_PROG$ACQUISITION_IMMOBILISATION<-DCF_PROG$FraisDevProsp_AcqApCreat+DCF_PROG$FraisDevProsp_VirepostPost+DCF_PROG$BrevLicLogDroiSim_AcqApCreat+
  DCF_PROG$BrevLicLogDroiSim_VirepostPost+ DCF_PROG$FdsCommDroiB_AcqApCreat+DCF_PROG$FdsCommDroiB_VirepostPost+DCF_PROG$AutrImmbIncorp_AcqApCreat+
  DCF_PROG$AutrImmbIncorp_VirepostPost+DCF_PROG$TerrHorsImmePlac_AcqApCreat+DCF_PROG$TerrHorsImmePlac_VirepostPost+DCF_PROG$TerrImmePlac_AcqApCreat+
  DCF_PROG$TerrImmePlac_VirepostPost+DCF_PROG$BatHorsImmePlac_AcqApCreat+DCF_PROG$BatHorsImmePlac_VirepostPost+DCF_PROG$BatImmePlac_AcqApCreat+DCF_PROG$BatImmePlac_VirepostPost+
  DCF_PROG$amenagAgenInst_AcqApCreat+DCF_PROG$amenagAgenInst_VirepostPost+DCF_PROG$MatMobActBiol_AcqApCreat+DCF_PROG$MatMobActBiol_VirepostPost+DCF_PROG$MatTransp_AcqApCreat+DCF_PROG$MatTransp_VirepostPost

IND_6<-function(criticite,numerateur,denominateur,coeff,x1,x2,x3,x4){
  seuil<-(denominateur*0.4)
  
  if(denominateur!=0 & !is.na(denominateur) & !is.na(numerateur)){
    indicateur<-(numerateur/denominateur)
  }
  else{
    indicateur<-0
  }
  
  if(indicateur<seuil){
    groupe="vert"
    ecart=0
    score=0
    RISQUE<-c(ecart,groupe,score)
  }
  else
  {
    ecart<-(seuil*denominateur)-numerateur
    ecart=ecart*coeff
    if(ecart<x1){
      impact=1
    }
    else if(ecart<x2){
      impact=2
    }
    else if(ecart<x3){
      impact<-3
    }
    else if(ecart<x4){
      impact<-4
    }
    else{
      impact<-5
    }
    score<-criticite*impact
    groupe=switch (as.character(score),
                   '1' = 'vert',
                   '2' = 'vert',
                   '3' = 'vert',
                   '4' = 'vert',
                   '5' = ifelse(as.character(criticite)=="1","vert","jaune"),
                   '6' = ifelse(as.character(criticite)=="2","vert","jaune"),
                   '8' = 'jaune',
                   '9' = 'jaune',
                   '10' = ifelse(as.character(criticite)=="2","jaune","rouge"),
                   '12' = 'orange',
                   '15' = ifelse(as.character(criticite)=="3","orange","rouge"),
                   '16' = 'orange',
                   '20' = 'rouge',
                   '25' = 'rouge'
                   
    )
    RISQUE=c(ecart,groupe,score)
  }
  return(RISQUE)
}

##################### CALCUL DE L'INDICATEUR NUMERO 6 
for (i in 1:nrow(DCF_PROG)){
  if(!is.na(DCF_PROG$ACQUISITION_IMMOBILISATION[i]) & !is.na(DCF_PROG$XB_CA_31_12_N_Net[i])){
    BD_TVA_Shiny$RISQUE_IND_6[i]=IND_6(5,DCF_PROG$ACQUISITION_IMMOBILISATION[i],DCF_PROG$XB_CA_31_12_N_Net[i],0.9,500000,5000000,20000000,100000000)[2]
    BD_TVA_Shiny$GAP_IND_6[i]=IND_6(5,DCF_PROG$ACQUISITION_IMMOBILISATION[i],DCF_PROG$XB_CA_31_12_N_Net[i],0.9,500000,5000000,20000000,100000000)[1]
    BD_TVA_Shiny$SCORE_IND_6[i]=IND_6(5,DCF_PROG$ACQUISITION_IMMOBILISATION[i],DCF_PROG$XB_CA_31_12_N_Net[i],0.9,500000,5000000,20000000,100000000)[3]
  }
  else{
    BD_TVA_Shiny$RISQUE_IND_6[i]="Non disponible"
    BD_TVA_Shiny$GAP_IND_6[i]=0
    BD_TVA_Shiny$SCORE_IND_6[i]=0
  }
}

##################### CALCUL DE L'INDICATEUR NUMERO 7 A 
for (i in 1:nrow(DCF_PROG)){
  if(!is.na(DCF_PROG$EXPORT_NOMBRE_TITRE[i])){
    BD_TVA_Shiny$RISQUE_IND_7_A[i]=ifelse(DCF_PROG$EXPORT_NOMBRE_TITRE[i]>5,"rouge","vert")
  }
  else{
    BD_TVA_Shiny$RISQUE_IND_7_A[i]="Non disponible"
    
  }
}

##################### CALCUL DE L'INDICATEUR NUMERO 7 B
for (i in 1:nrow(DCF_PROG)){
  if(!is.na(DCF_PROG$EXPORT_NOMBRE_TITRE[i])){
    BD_TVA_Shiny$RISQUE_IND_7_B[i]=ifelse(DCF_PROG$IMPORT_NOMBRE_TITRE[i]>5,"rouge","vert")
  }
  else{
    BD_TVA_Shiny$RISQUE_IND_7_B[i]="Non disponible"
    
  }
}



############## INDICATEUR 8 

TVA_IND8 <- function(criticite, numerateur, denominateur, seuil, coeff, x1, x2, x3, x4) {
  
  # Initialisation avec des valeurs par défaut NA
  ecart <- NA_real_
  groupe <- NA_character_
  score <- NA_real_
  
  # Vérification des données d'entrée
  if (is.na(numerateur) | is.na(denominateur)) {
    return(list(ecart = NA_real_, groupe = "Données manquantes", score = NA_real_))
  }
  
  # Cas spécial : dénominateur nul
  if (denominateur == 0) {
    return(list(
      ecart = NA_real_, 
      groupe = "Ratio non calculable", 
      score = NA_real_
    ))
  }
  
  # Calcul de l'indicateur
  indicateur <- numerateur / denominateur
  
  # Comparaison avec le seuil
  if (seuil<indicateur) {
    groupe <- "vert"
    ecart <- 0
    score <- 0
  } else {
    # Calcul de l'écart pondéré
    ecart <- abs((seuil * denominateur) - numerateur) * coeff
    
    # Détermination de l'impact
    impact <- dplyr::case_when(
      ecart < x1 ~ 1,
      ecart < x2 ~ 2,
      ecart < x3 ~ 3,
      ecart < x4 ~ 4,
      TRUE ~ 5
    )
    
    # Calcul du score
    score <- criticite * impact
    
    # Détermination du groupe de risque
    groupe <- dplyr::case_when(
      score <= 4 ~ "vert",
      score %in% c(5, 6) & criticite %in% c(1, 2) ~ "vert",
      score %in% c(5, 6) ~ "jaune",
      score %in% c(8, 9) ~ "jaune",
      score == 10 & criticite == 2 ~ "jaune",
      score == 10 ~ "rouge",
      score %in% c(12, 16) ~ "orange",
      score == 15 & criticite == 3 ~ "orange",
      score == 15 ~ "rouge",
      score >= 20 ~ "rouge",
      TRUE ~ "indéterminé"
    )
  }
  
  # Retour sous forme de liste nommée (plus lisible que vecteur)
  return(list(ecart = ecart, groupe = groupe, score = score))
}

#===================== Appliquer la fonction à chaque ligne du dataframe

for (i in 1:nrow(DCF_PROG)) {
  # Appeler la fonction une seule fois
  resultat <- TVA_IND8(
    4, 
    DCF_PROG$MONTANT_DECLARE[i], 
    DCF_PROG$Fourn_TVA_DEDUCTIBLE_AN[i], 
    0.95, 0.75, 
    500000, 5000000, 20000000, 100000000
  )
  
  # Extraire les résultats
  BD_TVA_Shiny$RISQUE_IND_8[i] <- resultat$groupe
  BD_TVA_Shiny$GAP_IND_8[i] <- resultat$ecart
  BD_TVA_Shiny$SCORE_IND_8[i] <- resultat$score
}

####################### INDICATEUR 9 
TVA_IND9<-function(criticite,marge,seuil){
  if(!is.na(marge) & marge<seuil){
    groupe<-"rouge"
  }
  else if(!is.na(marge) & marge>=seuil){
    groupe<-"vert"
  }
  else{
    groupe<-"Non disponible"
  }
  return(groupe) 
}

#CALCUL DE L'INDICATEUR DU RISQUE DE DELIVRANCE DE FAUSSE FACTURE, IND_1
for (i in 1:nrow(DCF_PROG)){
  if(!is.na(DCF_PROG$XA_MargCommerc_31_12_N_Net[i])){
    BD_TVA_Shiny$RISQUE_IND_9[i]=TVA_IND9(5,DCF_PROG$XA_MargCommerc_31_12_N_Net[i],0)
  }else{
    BD_TVA_Shiny$RISQUE_IND_9[i]="Non disponible"
  }
}

################# INDICATEUR 10 
TVA_IND10<-function(criticite,numerateur,denominateur,seuil,coeff,x1,x2,x3,x4){
  if(denominateur!=0 & !is.na(denominateur) & !is.na(numerateur)){
    indicateur<-(numerateur/denominateur)
  }
  else{
    indicateur<-0
  }
  
  if(indicateur>seuil){
    groupe="vert"
    ecart=0
    score=0
    RISQUE<-c(ecart,groupe,score)
  }
  else
  {
    ecart<-abs((seuil*denominateur)-numerateur)
    ecart=ecart*coeff
    if(ecart<x1){
      impact=1
    }
    else if(ecart<x2){
      impact=2
    }
    else if(ecart<x3){
      impact<-3
    }
    else if(ecart<x4){
      impact<-4
    }
    else{
      impact<-5
    }
    score<-criticite*impact
    groupe=switch (as.character(score),
                   '1' = 'vert',
                   '2' = 'vert',
                   '3' = 'vert',
                   '4' = 'vert',
                   '5' = ifelse(as.character(criticite)=="1","vert","jaune"),
                   '6' = ifelse(as.character(criticite)=="2","vert","jaune"),
                   '8' = 'jaune',
                   '9' = 'jaune',
                   '10' = ifelse(as.character(criticite)=="2","jaune","rouge"),
                   '12' = 'orange',
                   '15' = ifelse(as.character(criticite)=="3","orange","rouge"),
                   '16' = 'orange',
                   '20' = 'rouge',
                   '25' = 'rouge'
                   
    )
    RISQUE=c(ecart,groupe,score)
  }
  return(RISQUE)
}

#CALCUL DE L'INDICATEUR DU RISQUE DE DELIVRANCE DE FAUSSE FACTURE, IND_1
for (i in 1:nrow(DCF_PROG)){
  if((!is.na(DCF_PROG$IMPORT_TVA[i])) & (!is.na(DCF_PROG$TVA_SUPPORTE_IMPORT[i]))){
    
    BD_TVA_Shiny$RISQUE_IND_10[i]=TVA_IND10(4,DCF_PROG$TVA_SUPPORTE_IMPORT[i],DCF_PROG$IMPORT_TVA[i],0.95,0.75,500000,5000000,20000000,100000000)[2]
    BD_TVA_Shiny$GAP_IND_10[i]=TVA_IND10(4,DCF_PROG$TVA_SUPPORTE_IMPORT[i],DCF_PROG$IMPORT_TVA[i],0.95,0.75,500000,5000000,20000000,100000000)[1]
    BD_TVA_Shiny$SCORE_IND_10[i]=TVA_IND10(4,DCF_PROG$TVA_SUPPORTE_IMPORT[i],DCF_PROG$IMPORT_TVA[i],0.95,0.75,500000,5000000,20000000,100000000)[3]
    
  }
  else{
    BD_TVA_Shiny$RISQUE_IND_10[i]="Non disponible"
    BD_TVA_Shiny$GAP_IND_10[i]=0
    BD_TVA_Shiny$SCORE_IND_10[i]=0
    
  }
  
}


################# INDICATEUR 12 


# Fonction pour le calcul DU RISQUE DE DELIVRANCE DE FAUSSE FACTURE

TVA_IND12<-function(criticite,numerateur,denominateur,seuil,coeff,x1,x2,x3,x4){
  if(denominateur!=0 & !is.na(denominateur) & !is.na(numerateur)){
    indicateur<-(numerateur/denominateur)
  }
  else{
    indicateur<-0
  }
  
  if(seuil<indicateur){
    groupe="vert"
    ecart=0
    score=0
    RISQUE<-c(ecart,groupe,score)
  }
  else
  {
    ecart<-abs((seuil*denominateur)-numerateur)
    ecart=ecart*coeff
    if(ecart<x1){
      impact=1
    }
    else if(ecart<x2){
      impact=2
    }
    else if(ecart<x3){
      impact<-3
    }
    else if(ecart<x4){
      impact<-4
    }
    else{
      impact<-5
    }
    score<-criticite*impact
    groupe=switch (as.character(score),
                   '1' = 'vert',
                   '2' = 'vert',
                   '3' = 'vert',
                   '4' = 'vert',
                   '5' = ifelse(as.character(criticite)=="1","vert","jaune"),
                   '6' = ifelse(as.character(criticite)=="2","vert","jaune"),
                   '8' = 'jaune',
                   '9' = 'jaune',
                   '10' = ifelse(as.character(criticite)=="2","jaune","rouge"),
                   '12' = 'orange',
                   '15' = ifelse(as.character(criticite)=="3","orange","rouge"),
                   '16' = 'orange',
                   '20' = 'rouge',
                   '25' = 'rouge'
                   
    )
    RISQUE=c(ecart,groupe,score)
  }
  return(RISQUE)
}


#CALCUL DE L'INDICATEUR DU RISQUE DE DELIVRANCE DE FAUSSE FACTURE, IND_1

for (i in 1:nrow(DCF_PROG)){
  if(!is.na(DCF_PROG$EtatTVA_AnneeN[i]) & !is.na(DCF_PROG$TVA_DECEMBRE[i])){
    BD_TVA_Shiny$RISQUE_IND_12[i]=TVA_IND12(3,DCF_PROG$EtatTVA_AnneeN[i],DCF_PROG$TVA_DECEMBRE[i],0.2,0.5,500000,5000000,20000000,100000000)[2]
    BD_TVA_Shiny$GAP_IND_12[i]=TVA_IND12(3,DCF_PROG$EtatTVA_AnneeN[i],DCF_PROG$TVA_DECEMBRE[i],0.2,0.5,500000,5000000,20000000,100000000)[1]
    BD_TVA_Shiny$SCORE_IND_12[i]=TVA_IND12(3,DCF_PROG$EtatTVA_AnneeN[i],DCF_PROG$TVA_DECEMBRE[i],0.2,0.5,500000,5000000,20000000,100000000)[3]
    
  }
  else{
    BD_TVA_Shiny$RISQUE_IND_12[i]="Non disponible"
    BD_TVA_Shiny$GAP_IND_12[i]=0
    BD_TVA_Shiny$SCORE_IND_12[i]=0
  }
}




####################### INDICATEUR 13 
# Montant de la base imposable ÃÂ  la TVA
DCF_PROG <- DCF_PROG %>%
  mutate(MNT_BASE_IMPOSABLE =OP_TAXBLE_COURANTE_AUTRES_OPERATIONS_TAXABLES_04+OP_TAXBLE_COURANTE_CESSION_IMMO_03+
           OP_TAXBLE_COURANTE_LIVRAISON_A_SOI_MEME_02+OP_TAXBLE_COURANTE_OPERATION_TAXABLE_TAUX_10PC_218+
           OP_TAXBLE_COURANTE_VENTES_PRESTATIONS_SERVICES_TRVX_IMMO_01+OP_TAXBLE_MARCHE_CDE_10PC_220+
           OP_TAXBLE_MARCHE_CDE_AUTRES_OP_TAXABLES_08+OP_TAXBLE_MARCHE_CDE_PRESTATION_SERVICES_06+
           OP_TAXBLE_MARCHE_CDE_TRAVAUX_IMMO_TRVX_PUPLIC_07+OP_TAXBLE_MARCHE_CDE_VENTES_05)

TVA_IND13<-function(criticite,numerateur,denominateur,seuil,coeff,x1,x2,x3,x4){
  if(denominateur!=0 & !is.na(denominateur) & !is.na(numerateur)){
    indicateur<-numerateur/denominateur
  }
  else{
    indicateur<-0
  }
  if(indicateur>seuil){
    groupe="vert"
    ecart=0
    score=0
    RISQUE<-c(ecart,groupe,score)
  }
  else
  {
    ecart<-abs(denominateur-numerateur)
    ecart=ecart*coeff
    if(ecart<x1){
      impact=1
    }
    else if(ecart<x2){
      impact=2
    }
    else if(ecart<x3){
      impact<-3
    }
    else if(ecart<x4){
      impact<-4
    }
    else{
      impact<-5
    }
    score<-criticite*impact
    groupe=switch (as.character(score),
                   '1' = 'vert',
                   '2' = 'vert',
                   '3' = 'vert',
                   '4' = 'vert',
                   '5' = ifelse(as.character(criticite)=="1","vert","jaune"),
                   '6' = ifelse(as.character(criticite)=="2","vert","jaune"),
                   '8' = 'jaune',
                   '9' = 'jaune',
                   '10' = ifelse(as.character(criticite)=="2","jaune","rouge"),
                   '12' = 'orange',
                   '15' = ifelse(as.character(criticite)=="3","orange","rouge"),
                   '16' = 'orange',
                   '20' = 'rouge',
                   '25' = 'rouge'
                   
    )
    RISQUE=c(ecart,groupe,score)
  }
  return(RISQUE)
}

#CALCUL DE L'INDICATEUR DU RISQUE DE DELIVRANCE DE FAUSSE FACTURE, IND_1
for (i in 1:nrow(DCF_PROG)){
  if(!is.na(DCF_PROG$MNT_BASE_IMPOSABLE[i]) & !is.na(DCF_PROG$XB_CA_31_12_N_Net[i])){
    BD_TVA_Shiny$RISQUE_IND_13[i]=TVA_IND13(3,DCF_PROG$MNT_BASE_IMPOSABLE[i],DCF_PROG$XB_CA_31_12_N_Net[i],1,0.5,500000,5000000,20000000,100000000)[2]
    BD_TVA_Shiny$GAP_IND_13[i]=TVA_IND13(3,DCF_PROG$MNT_BASE_IMPOSABLE[i],DCF_PROG$XB_CA_31_12_N_Net[i],1,0.5,500000,5000000,20000000,100000000)[1]
    BD_TVA_Shiny$SCORE_IND_13[i]=TVA_IND13(3,DCF_PROG$MNT_BASE_IMPOSABLE[i],DCF_PROG$XB_CA_31_12_N_Net[i],1,0.5,500000,5000000,20000000,100000000)[3]
    
  }
  else{
    BD_TVA_Shiny$RISQUE_IND_13[i]="Non disponible"
    BD_TVA_Shiny$GAP_IND_13[i]=0
    BD_TVA_Shiny$SCORE_IND_13[i]=0
    
  }
}
# Le calcul DU RISQUE DE DELIVRANCE DE FAUSSE FACTURE

for (i in 1:nrow(DCF_PROG)) {
  # Vérifier les conditions de base
  if (DCF_PROG$CODE_REG_FISC[i] %in% c("RN", "ND")) {
    
    # Vérifier si la somme des 12 périodes est disponible
    if (!is.na(DCF_PROG$MONTANT_TOTAL_LA_BRUTE_15[i])) {
      
      if (DCF_PROG$MONTANT_TOTAL_LA_BRUTE_15[i] == 0) {
        BD_TVA_Shiny$RISQUE_IND_14[i] <- "rouge"
        #BD_TVA_Shiny$SCORE_IND_14[i] <- 3
        #BD_TVA_Shiny$COMMENTAIRE_IND_14[i] <- "Absence de CA sur 12 déclarations"
      } else {
        BD_TVA_Shiny$RISQUE_IND_14[i] <- "vert"
        #BD_TVA_Shiny$SCORE_IND_14[i] <- 0
        #BD_TVA_Shiny$COMMENTAIRE_IND_14[i] <- "CA présent"
      }
      
    } else {
      # Données manquantes
      BD_TVA_Shiny$RISQUE_IND_14[i] <- "Données manquantes"
      #BD_TVA_Shiny$SCORE_IND_14[i] <- NA_real_
      #BD_TVA_Shiny$COMMENTAIRE_IND_14[i] <- "Somme CA 12 périodes non calculable"
    }
    
  } else {
    # Hors régime concerné
    BD_TVA_Shiny$RISQUE_IND_14[i] <- "Régime fiscal non concerné"
    #BD_TVA_Shiny$SCORE_IND_14[i] <- NA_real_
    #BD_TVA_Shiny$COMMENTAIRE_IND_14[i] <- "Régime fiscal non concerné"
  }
}

# Identifier les entreprises avec une date de programmation supérieure à 3 ans et un avis de vérification renseigné
BD_TVA_Shiny$RISQUE_IND_16 <- ifelse((DCF_PROG$DATE_DERNIERE_VG <'2020-12-31') & is.na(DCF_PROG$DATE_DERNIERE_AVIS), "rouge", "vert")





############## INDICATEUR NUMERO 17 
#Prommoteur de plusieurs entreprises

# for(i in 1:nrow(DCF_PROG)){
#   if(!is.na(DCF_PROG$Occurrences_IFU[i])){
#     BD_TVA_Shiny$RISQUE_IND_17[i]<-ifelse(DCF_PROG$Occurrences_IFU[i]>5,"rouge","orange")
#   }
#   
# }


# ######lien entre fournisseurs
# BD_TVA_Shiny$IFUANNEE=paste0(BD_TVA_Shiny$NUM_IFU,BD_TVA_Shiny$ANNEE_FISCAL)
# BD_TVA_Shiny$RISQUE_IND_19=ifelse((BD_TVA_Shiny$IFUANNEE %in% c(IFU_CLI_RISQ,IFU_FOURN_RISQ)),"rouge","vert")


##########################INDICATEUR 20

TVA_IND20<-function(criticite,numerateur,denominateur,seuil,coeff,x1,x2,x3,x4){
  if(denominateur!=0 & !is.na(denominateur) & !is.na(numerateur)){
    indicateur<-(numerateur/denominateur)
  }
  else{
    indicateur<-0
  }
  
  if(seuil<indicateur){
    groupe="vert"
    ecart=0
    score=0
    RISQUE<-c(ecart,groupe,score)
  }
  else
  {
    ecart<-abs((seuil*denominateur)-numerateur)
    ecart=ecart*coeff
    if(ecart<x1){
      impact=1
    }
    else if(ecart<x2){
      impact=2
    }
    else if(ecart<x3){
      impact<-3
    }
    else if(ecart<x4){
      impact<-4
    }
    else{
      impact<-5
    }
    score<-criticite*impact
    groupe=switch (as.character(score),
                   '1' = 'vert',
                   '2' = 'vert',
                   '3' = 'vert',
                   '4' = 'vert',
                   '5' = ifelse(as.character(criticite)=="1","vert","jaune"),
                   '6' = ifelse(as.character(criticite)=="2","vert","jaune"),
                   '8' = 'jaune',
                   '9' = 'jaune',
                   '10' = ifelse(as.character(criticite)=="2","jaune","rouge"),
                   '12' = 'orange',
                   '15' = ifelse(as.character(criticite)=="3","orange","rouge"),
                   '16' = 'orange',
                   '20' = 'rouge',
                   '25' = 'rouge'
                   
    )
    RISQUE=c(ecart,groupe,score)
  }
  return(RISQUE)
}


#CALCUL DE L'INDICATEUR DU RISQUE DE DELIVRANCE DE FAUSSE FACTURE, IND_1

for (i in 1:nrow(DCF_PROG)){
  if(!is.na(DCF_PROG$BENEFICE_IMPOSABLE[i]) & !is.na(DCF_PROG$CA_HTVA[i])){
    BD_TVA_Shiny$RISQUE_IND_20[i]=TVA_IND20(2,DCF_PROG$BENEFICE_IMPOSABLE[i],DCF_PROG$CA_HTVA[i],0.2,0.5,500000,5000000,20000000,100000000)[2]
    BD_TVA_Shiny$GAP_IND_20[i]=TVA_IND20(2,DCF_PROG$BENEFICE_IMPOSABLE[i],DCF_PROG$CA_HTVA[i],0.2,0.5,500000,5000000,20000000,100000000)[1]
    BD_TVA_Shiny$SCORE_IND_20[i]=TVA_IND20(2,DCF_PROG$BENEFICE_IMPOSABLE[i],DCF_PROG$CA_HTVA[i],0.2,0.5,500000,5000000,20000000,100000000)[3]
    
  }
  else{
    BD_TVA_Shiny$RISQUE_IND_20[i]="Non disponible"
    BD_TVA_Shiny$GAP_IND_20[i]=0
    BD_TVA_Shiny$SCORE_IND_20[i]=0
  }
}

unique(BD_TVA_Shiny$RISQUE_IND_20)


################# INDICATEUR 21 

# Calcul des numerateurs

# Calcule de la première variaion 
DCF_PROG <-transform(DCF_PROG,numerateur_21_1= ave(BI_Clients_Exer31_12_N_Net, NUM_IFU, FUN = function(x) c(NA, diff(x))))

#Somme des lignes XG et XH
DCF_PROG$SOMME_XG_XH<-DCF_PROG$XG_RESULT_AO_31_12_N_Net+DCF_PROG$XH_RESULTAT_HAO_31_12_N_Net

## Calcule de la deuxième variaion 
DCF_PROG <-transform(DCF_PROG,numerateur_21_2= ave(SOMME_XG_XH, NUM_IFU, FUN = function(x) c(NA, diff(x))))

# Calcul des ratios
for (i in 1:nrow(DCF_PROG)){
  if(DCF_PROG$CA_HTVA[i]!=0 & !is.na(DCF_PROG$CA_HTVA[i]) & !is.na(DCF_PROG$numerateur_21_1[i])){
    DCF_PROG$RATIO_21_1[i]=DCF_PROG$numerateur_21_1[i]/DCF_PROG$CA_HTVA[i]
  }
  else{
    DCF_PROG$RATIO_21_1[i]=0
  }
}

# Determination du risque
for (i in 1:nrow(DCF_PROG)){
  if(DCF_PROG$XB_CA_31_12_N_Net[i]!=0 & !is.na(DCF_PROG$XB_CA_31_12_N_Net[i]) & !is.na(DCF_PROG$numerateur_21_2[i])){
    DCF_PROG$RATIO_21_2[i]=DCF_PROG$numerateur_21_2[i]/DCF_PROG$XB_CA_31_12_N_Net[i]
  }
  else{
    DCF_PROG$RATIO_21_2[i]=0
  }
}

# Determination du risque
for (i in 1:nrow(DCF_PROG)){
  if(DCF_PROG$RATIO_21_1[i]<0 & DCF_PROG$RATIO_21_2[i]>0){
    BD_TVA_Shiny$RISQUE_IND_21[i]="rouge"
  }
  else{
    BD_TVA_Shiny$RISQUE_IND_21[i]="vert"
  }
}

#################### Indicateur 23 

# Calcule de la première variaion 
DCF_PROG <-transform(DCF_PROG,numerateur_23= ave(XA_MargCommerc_31_12_N_Net, NUM_IFU, FUN = function(x) c(NA, diff(x))))

# Calcul de la fonction
IND_23<-function(numerateur,denominateur){
  if(denominateur!=0 & !is.na(denominateur) & !is.na(numerateur)){
    indicateur<-(numerateur/denominateur)
  }
  else{
    indicateur<-0
  }
  
  if(indicateur<0){
    groupe="rouge"
  }
  else{
    groupe="vert"
  }
  return(groupe)
}

# Determination du risque
for (i in 1:nrow(DCF_PROG)){
  if(!is.na(DCF_PROG$XB_CA_31_12_N_Net[i]) & !is.na(DCF_PROG$numerateur_23[i])){
    BD_TVA_Shiny$RISQUE_IND_23[i]=IND_23(DCF_PROG$numerateur_23[i],DCF_PROG$XB_CA_31_12_N_Net[i])
  }
  else{
    BD_TVA_Shiny$RISQUE_IND_23[i]="Non disponible"
  }
}



##################### CALCUL INDICATEUR 24  


## Calcul du ratio de l'indicatteur 24 
for (i in 1:nrow(DCF_PROG)){
  
  if(!is.na(DCF_PROG$XA_MargCommerc_31_12_N_Net[i]) & !is.na(DCF_PROG$XB_CA_31_12_N_Net[i])& (DCF_PROG$XA_MargCommerc_31_12_N_Net[i]!=0) & (DCF_PROG$XB_CA_31_12_N_Net[i]!=0)){
    DCF_PROG$RATIO_24[i]<-DCF_PROG$XA_MargCommerc_31_12_N_Net[i]/DCF_PROG$XB_CA_31_12_N_Net[i]
    
  }
  else{
    DCF_PROG$RATIO_24[i]<-0
  }
  
}
######## Calcul des medianes 

# MEDIANE_RATIO_24
DCF_PROG_med24=subset(DCF_PROG,DCF_PROG$RATIO_24!=0)


DCF_PROG_med24=DCF_PROG_med24%>%
  group_by(CODE_SECT_ACT,ANNEE_FISCAL)%>%
  mutate(MEDIAN_RATIO_24=median(RATIO_24,na.rm=T))

DCF_PROG_med24=DCF_PROG_med24[,c("NUM_IFU","CODE_SECT_ACT","ANNEE_FISCAL","MEDIAN_RATIO_24")]
DCF_PROG_med24=DCF_PROG_med24[!duplicated(DCF_PROG_med24$NUM_IFU),]

#la jointure afin d'avoir les lignes supprimées 
DCF_PROG<-left_join(DCF_PROG,DCF_PROG_med24,by = c("NUM_IFU","CODE_SECT_ACT","ANNEE_FISCAL"))



# Fonction pour le calcul DES INDICAEURS LIES A LA MEDIANE
IND_24_25_26<-function(criticite,numerateur,denominateur,seuil,coeff,x1,x2,x3,x4){
  if(denominateur!=0 & !is.na(denominateur) & !is.na(numerateur)){
    indicateur<-(numerateur/denominateur)
  }
  else{
    indicateur<-0
  }
  
  if(seuil<indicateur){
    groupe="vert"
    ecart=0
    score=0
    RISQUE<-c(ecart,groupe,score)
  }
  else
  {
    ecart<-abs((seuil*denominateur)-numerateur)
    ecart=ecart*coeff
    if(ecart<x1){
      impact=1
    }
    else if(ecart<x2){
      impact=2
    }
    else if(ecart<x3){
      impact<-3
    }
    else if(ecart<x4){
      impact<-4
    }
    else{
      impact<-5
    }
    score<-criticite*impact
    groupe=switch (as.character(score),
                   '1' = 'vert',
                   '2' = 'vert',
                   '3' = 'vert',
                   '4' = 'vert',
                   '5' = ifelse(as.character(criticite)=="1","vert","jaune"),
                   '6' = ifelse(as.character(criticite)=="2","vert","jaune"),
                   '8' = 'jaune',
                   '9' = 'jaune',
                   '10' = ifelse(as.character(criticite)=="2","jaune","rouge"),
                   '12' = 'orange',
                   '15' = ifelse(as.character(criticite)=="3","orange","rouge"),
                   '16' = 'orange',
                   '20' = 'rouge',
                   '25' = 'rouge'
                   
    )
    RISQUE=c(ecart,groupe,score)
  }
  return(RISQUE)
}

#CALCUL DE L'INDICATEUR 24
for (i in 1:nrow(DCF_PROG)){
  if(!is.na(DCF_PROG$XA_MargCommerc_31_12_N_Net[i]) & !is.na(DCF_PROG$XB_CA_31_12_N_Net[i]) & !is.na(DCF_PROG$MEDIAN_RATIO_24[i]) & (DCF_PROG$MEDIAN_RATIO_24[i]!=0)){
    BD_TVA_Shiny$RISQUE_IND_24[i]=IND_24_25_26(4,DCF_PROG$XA_MargCommerc_31_12_N_Net[i],DCF_PROG$XB_CA_31_12_N_Net[i],DCF_PROG$MEDIAN_RATIO_24[i]-0.1,0.3,500000,5000000,20000000,100000000)[2]
    BD_TVA_Shiny$GAP_IND_24[i]=IND_24_25_26(4,DCF_PROG$XA_MargCommerc_31_12_N_Net[i],DCF_PROG$XB_CA_31_12_N_Net[i],DCF_PROG$MEDIAN_RATIO_24[i]-0.1,0.3,500000,5000000,20000000,100000000)[1]
    BD_TVA_Shiny$SCORE_IND_24[i]=IND_24_25_26(4,DCF_PROG$XA_MargCommerc_31_12_N_Net[i],DCF_PROG$XB_CA_31_12_N_Net[i],DCF_PROG$MEDIAN_RATIO_24[i]-0.1,0.3,500000,5000000,20000000,100000000)[3]
  }
  else{
    BD_TVA_Shiny$RISQUE_IND_24[i]="Non disponible"
    BD_TVA_Shiny$GAP_IND_24[i]=0
    BD_TVA_Shiny$SCORE_IND_24[i]=0
  }
}

###################### INDICATEUR NUMERO 25 

# Calcul du ratio de l'indicatteur 24 
for (i in 1:nrow(DCF_PROG)){
  
  if(!is.na(DCF_PROG$XC_VALEUR_AJOUTEE_31_12_N_Net[i]) & !is.na(DCF_PROG$XB_CA_31_12_N_Net[i])& (DCF_PROG$XB_CA_31_12_N_Net[i]!=0) & (DCF_PROG$XB_CA_31_12_N_Net[i]!=0)){
    DCF_PROG$RATIO_25[i]<-DCF_PROG$XC_VALEUR_AJOUTEE_31_12_N_Net[i]/DCF_PROG$XB_CA_31_12_N_Net[i]
    
  }
  else{
    DCF_PROG$RATIO_25[i]<-0
  }
  
}


# MEDIANE_RATIO_25
DCF_PROG_med25=subset(DCF_PROG,DCF_PROG$RATIO_25!=0)


DCF_PROG_med25=DCF_PROG_med25%>%
  group_by(CODE_SECT_ACT,ANNEE_FISCAL)%>%
  mutate(MEDIAN_RATIO_25=median(RATIO_25,na.rm=T))

DCF_PROG_med25=DCF_PROG_med25[,c("NUM_IFU","CODE_SECT_ACT","ANNEE_FISCAL","MEDIAN_RATIO_25")]
DCF_PROG_med25=DCF_PROG_med25[!duplicated(DCF_PROG_med25$NUM_IFU,DCF_PROG_med25$ANNEE_FISCAL),]

#la jointure afin d'avoir les lignes supprimées 
DCF_PROG<-left_join(DCF_PROG,DCF_PROG_med25,by = c("NUM_IFU","CODE_SECT_ACT","ANNEE_FISCAL"))
#DCF_PROG$MEDIAN_RATIO_25=DCF_PROG$MEDIAN_RATIO_25.y

#CALCUL DE L'INDICATEUR 
for (i in 1:nrow(DCF_PROG)){
  if(!is.na(DCF_PROG$XC_VALEUR_AJOUTEE_31_12_N_Net[i]) & !is.na(DCF_PROG$XB_CA_31_12_N_Net[i]) & (DCF_PROG$XB_CA_31_12_N_Net[i]!=0) & (DCF_PROG$MEDIAN_RATIO_25[i]!=0) & !is.na(DCF_PROG$MEDIAN_RATIO_25[i])){
    BD_TVA_Shiny$RISQUE_IND_25[i]=IND_24_25_26(4,DCF_PROG$XC_VALEUR_AJOUTEE_31_12_N_Net[i],DCF_PROG$XB_CA_31_12_N_Net[i],DCF_PROG$MEDIAN_RATIO_25[i]-0.05,0.2,500000,5000000,20000000,100000000)[2]
    BD_TVA_Shiny$GAP_IND_25[i]=IND_24_25_26(4,DCF_PROG$XC_VALEUR_AJOUTEE_31_12_N_Net[i],DCF_PROG$XB_CA_31_12_N_Net[i],DCF_PROG$MEDIAN_RATIO_25[i]-0.05,0.2,500000,5000000,20000000,100000000)[1]
    BD_TVA_Shiny$SCORE_IND_25[i]=IND_24_25_26(4,DCF_PROG$XC_VALEUR_AJOUTEE_31_12_N_Net[i],DCF_PROG$XB_CA_31_12_N_Net[i],DCF_PROG$MEDIAN_RATIO_25[i]-0.05,0.2,500000,5000000,20000000,100000000)[3]
  }
  else{
    BD_TVA_Shiny$RISQUE_IND_25[i]="Non disponible"
    BD_TVA_Shiny$GAP_IND_25[i]=0
    BD_TVA_Shiny$SCORE_IND_25[i]=0
  }
}

################## INDICATEUR 26 

DCF_PROG$MNT_TOTAL_NOTE_32=DCF_PROG$TOTAL_PdtionVenduePays_Valeur+DCF_PROG$TOTAL_PdtionVendueAutrPays_Valeur+
  DCF_PROG$TOTAL_PdtionVendueHorsOHADA_Valeur+DCF_PROG$TOTAL_PdtionImmobilise_Valeur
#Le ratio_26

# Calcul du ratio de l'indicatteur 24 
for (i in 1:nrow(DCF_PROG)){
  
  if(!is.na(DCF_PROG$XB_CA_31_12_N_Net[i]) & !is.na(DCF_PROG$MNT_TOTAL_NOTE_32[i])& (DCF_PROG$XB_CA_31_12_N_Net[i]!=0) & (DCF_PROG$MNT_TOTAL_NOTE_32[i]!=0)){
    DCF_PROG$RATIO_26<-DCF_PROG$MNT_TOTAL_NOTE_32[i]/DCF_PROG$XB_CA_31_12_N_Net[i]
    
  }
  else{
    DCF_PROG$RATIO_26[i]<-0
  }
  
}


# MEDIANE_RATIO_26
DCF_PROG_med26=subset(DCF_PROG,DCF_PROG$RATIO_26!=0)


DCF_PROG_med26=DCF_PROG_med26%>%
  group_by(CODE_SECT_ACT,ANNEE_FISCAL)%>%
  mutate(MEDIAN_RATIO_26=median(RATIO_26,na.rm=T))

DCF_PROG_med26=DCF_PROG_med26[,c("NUM_IFU","CODE_SECT_ACT","ANNEE_FISCAL","MEDIAN_RATIO_26")]
DCF_PROG_med26=DCF_PROG_med26[!duplicated(DCF_PROG_med26$NUM_IFU,DCF_PROG_med26$ANNEE_FISCAL),]

#la jointure afin d'avoir les lignes supprimées 
DCF_PROG<-left_join(DCF_PROG,DCF_PROG_med26,by = c("NUM_IFU","CODE_SECT_ACT","ANNEE_FISCAL"))
#DCF_PROG$MEDIAN_RATIO_25=DCF_PROG$MEDIAN_RATIO_25.y




#CALCUL DE L'INDICATEUR 
for (i in 1:nrow(DCF_PROG)){
  if(!is.na(DCF_PROG$ID_TtlProductionExercice[i]) & !is.na(DCF_PROG$XB_CA_31_12_N_Net[i]) & !is.na(DCF_PROG$MEDIAN_RATIO_26[i])){
    BD_TVA_Shiny$RISQUE_IND_26[i]=IND_24_25_26(4,DCF_PROG$ID_TtlProductionExercice[i],DCF_PROG$XB_CA_31_12_N_Net[i],DCF_PROG$MEDIAN_RATIO_26[i]-0.1,0.2,500000,5000000,20000000,100000000)[2]
    BD_TVA_Shiny$GAP_IND_26[i]=IND_24_25_26(4,DCF_PROG$ID_TtlProductionExercice[i],DCF_PROG$XB_CA_31_12_N_Net[i],DCF_PROG$MEDIAN_RATIO_26[i]-0.1,0.2,500000,5000000,20000000,100000000)[1]
    BD_TVA_Shiny$SCORE_IND_26[i]=IND_24_25_26(4,DCF_PROG$ID_TtlProductionExercice[i],DCF_PROG$XB_CA_31_12_N_Net[i],DCF_PROG$MEDIAN_RATIO_26[i]-0.1,0.2,500000,5000000,20000000,100000000)[3]
  }
  else{
    BD_TVA_Shiny$RISQUE_IND_26[i]="Non disponible"
    BD_TVA_Shiny$GAP_IND_26[i]=0
    BD_TVA_Shiny$SCORE_IND_26[i]=0
  }
}


################indicateur 27

# Parcours les valeurs de la colonne et applique la condition if
for (i in 1:length(DCF_PROG$XD_EXCED_BRUT_EXPL_31_12_N_Net)) {
  if (!is.na(DCF_PROG$XD_EXCED_BRUT_EXPL_31_12_N_Net[i])& (DCF_PROG$XD_EXCED_BRUT_EXPL_31_12_N_Net[i]< 0)) {
    BD_TVA_Shiny$RISQUE_IND_27[i]<-"rouge"
  }
  else{
    BD_TVA_Shiny$RISQUE_IND_27[i]<-"vert"
  }
}

#################### Indicateur 28 

# Calcul des numerateurs
# DCF_PROG <-transform(DCF_PROG,numerateur_28_1= ave(XD_EXCED_BRUT_EXPL_31_12_N_Net, NUM_IFU, FUN = function(x) c(NA, diff(x))))
# DCF_PROG <-transform(DCF_PROG,numerateur_28_2= ave(RI_ImpotTaxe_31_12_N_Net, NUM_IFU, FUN = function(x) c(NA, diff(x))))
# DCF_PROG <-transform(DCF_PROG,numerateur_28_1= ave(DCF_PROG$TtlSubventions_AnneeN, NUM_IFU, FUN = function(x) c(NA, diff(x))))
# DCF_PROG <-transform(DCF_PROG,numerateur_28_1= ave(DCF_PROG$RK_ChargDePersonnel_31_12_N_Net, NUM_IFU, FUN = function(x) c(NA, diff(x))))

#################### Indicateur 29 
DCF_PROG <-transform(DCF_PROG,numerateur_29_1= ave(RL_DotAmortProviDep_31_12_N_Net, NUM_IFU, FUN = function(x) c(NA, diff(x))))
DCF_PROG <-transform(DCF_PROG,numerateur_29_2= ave(AZ_TtlActifImmob_Exer31_12_N_Brut, NUM_IFU, FUN = function(x) c(NA, diff(x))))

#Calcul des ratios 

for (i in 1:nrow(DCF_PROG)){
  if(DCF_PROG$XB_CA_31_12_N_Net[i]!=0 & !is.na(DCF_PROG$XB_CA_31_12_N_Net[i]) & !is.na(DCF_PROG$numerateur_29_1[i])){
    DCF_PROG$RATIO_29_1[i]=DCF_PROG$numerateur_29_1[i]/DCF_PROG$XB_CA_31_12_N_Net[i]
  }
  else{
    DCF_PROG$RATIO_29_1[i]=0
  }
}

for (i in 1:nrow(DCF_PROG)){
  if(DCF_PROG$XB_CA_31_12_N_Net[i]!=0 & !is.na(DCF_PROG$XB_CA_31_12_N_Net[i]) & !is.na(DCF_PROG$numerateur_29_2[i])){
    DCF_PROG$RATIO_29_2[i]=DCF_PROG$numerateur_29_2[i]/DCF_PROG$XB_CA_31_12_N_Net[i]
  }
  else{
    DCF_PROG$RATIO_29_2[i]=0
  }
}

# Determination du risque
for (i in 1:nrow(DCF_PROG)){
  if(DCF_PROG$RATIO_29_1[i]>0 & DCF_PROG$RATIO_29_2[i]<0){
    BD_TVA_Shiny$RISQUE_IND_29[i]="rouge"
  }
  else{
    BD_TVA_Shiny$RISQUE_IND_29[i]="vert"
  }
}


########## INDICATEUR 30 
DCF_PROG <-transform(DCF_PROG,VAR_XF= ave(XF_RESULT_FIN_31_12_N_Net, NUM_IFU, FUN = function(x) c(0, diff(x))))
DCF_PROG$SOM_PF<-DCF_PROG$TtlProviRisqCharg_AnneeN+DCF_PROG$DN_ProviRisqChargCourTerm_Exer31_12_N_Net
DCF_PROG <-transform(DCF_PROG,VAR_PF= ave(SOM_PF, NUM_IFU, FUN = function(x) c(0, diff(x))))

# Calcul des ratios
for (i in 1:nrow(DCF_PROG)){
  if(DCF_PROG$XB_CA_31_12_N_Net[i]!=0 & !is.na(DCF_PROG$XB_CA_31_12_N_Net[i]) & !is.na(DCF_PROG$VAR_XF[i])){
    DCF_PROG$RATIO_30_1[i]=DCF_PROG$VAR_XF[i]/DCF_PROG$XB_CA_31_12_N_Net[i]
  }
  else{
    DCF_PROG$RATIO_30_1[i]=0
  }
}

for (i in 1:nrow(DCF_PROG)){
  if(DCF_PROG$XB_CA_31_12_N_Net[i]!=0 & !is.na(DCF_PROG$XB_CA_31_12_N_Net[i]) & !is.na(DCF_PROG$VAR_PF[i])){
    DCF_PROG$RATIO_30_2[i]=DCF_PROG$VAR_PF[i]/DCF_PROG$XB_CA_31_12_N_Net[i]
  }
  else{
    DCF_PROG$RATIO_30_2[i]=0
  }
}

# Determination du risque
for (i in 1:nrow(DCF_PROG)){
  if(DCF_PROG$RATIO_30_1[i]<0 & DCF_PROG$RATIO_30_2[i]>0){
    BD_TVA_Shiny$RISQUE_IND_30[i]="rouge"
  }
  else{
    BD_TVA_Shiny$RISQUE_IND_30[i]="vert"
  }
}

##### INDICATEUR 32 

# Calcul des variations
DCF_PROG <-transform(DCF_PROG,VAR_XH= ave(XH_RESULTAT_HAO_31_12_N_Net, NUM_IFU, FUN = function(x) c(0, diff(x))))
DCF_PROG <-transform(DCF_PROG,VAR_RO= ave(RO_ValeurCptCessImmob_31_12_N_Net, NUM_IFU, FUN = function(x) c(0, diff(x))))

#Calcul du risque
# Determination du risque
for (i in 1:nrow(DCF_PROG)){
  if(!is.na(DCF_PROG$VAR_XH[i]) & !is.na(DCF_PROG$VAR_RO[i]) & DCF_PROG$VAR_XH[i]<0 & DCF_PROG$VAR_RO[i]>0){
    BD_TVA_Shiny$RISQUE_IND_32[i]="rouge"
  }
  else{
    BD_TVA_Shiny$RISQUE_IND_32[i]="vert"
  }
}

##### INDICATEUR 33 

# Calcul des variations
DCF_PROG <-transform(DCF_PROG,VAR_TO= ave(TO_AutresProdHAO_31_12_N_Net, NUM_IFU, FUN = function(x) c(0, diff(x))))
DCF_PROG <-transform(DCF_PROG,VAR_RP= ave(RP_AutresChargHAO_31_12_N_Net, NUM_IFU, FUN = function(x) c(0, diff(x))))

#Calcul du risque
# Determination du risque
for (i in 1:nrow(DCF_PROG)){
  if(!is.na(DCF_PROG$VAR_XH[i]) & !is.na(DCF_PROG$VAR_TO[i]) & !is.na(DCF_PROG$VAR_RP[i]) &  DCF_PROG$VAR_XH[i]<0 & DCF_PROG$VAR_TO[i]<0 & DCF_PROG$VAR_RP[i]>0){
    BD_TVA_Shiny$RISQUE_IND_33[i]="rouge"
  }
  else{
    BD_TVA_Shiny$RISQUE_IND_33[i]="vert"
  }
}


##### INDICATEUR 34 

# Calcul des variations
DCF_PROG <-transform(DCF_PROG,VAR_RK= ave(RK_ChargDePersonnel_31_12_N_Net, NUM_IFU, FUN = function(x) c(0, diff(x))))
DCF_PROG <-transform(DCF_PROG,VAR_CA_XB= ave(XB_CA_31_12_N_Net, NUM_IFU, FUN = function(x) c(0, diff(x))))

# Calcul du ratio
for (i in 1:nrow(DCF_PROG)){
  if(DCF_PROG$XB_CA_31_12_N_Net[i]!=0 & !is.na(DCF_PROG$XB_CA_31_12_N_Net[i]) & !is.na(DCF_PROG$VAR_RK[i])){
    DCF_PROG$RATIO_34[i]=DCF_PROG$VAR_RK[i]/DCF_PROG$XB_CA_31_12_N_Net[i]
  }
  else{
    DCF_PROG$RATIO_34[i]=0
  }
}
#Calcul du risque
# Determination du risque
for (i in 1:nrow(DCF_PROG)){
  if(!is.na(DCF_PROG$RATIO_34[i]) & !is.na(DCF_PROG$VAR_RK[i]) & !is.na(DCF_PROG$VAR_CA_XB[i]) &  DCF_PROG$RATIO_34[i]>0 & DCF_PROG$VAR_RK[i]>0 & DCF_PROG$VAR_CA_XB[i]<0){
    BD_TVA_Shiny$RISQUE_IND_34[i]="rouge"
  }
  else{
    BD_TVA_Shiny$RISQUE_IND_34[i]="vert"
  }
}




################ INDICATEUR 37

# Remplacer les NA dans BI_Clients_Exer31_12_N_Net par 0
DCF_PROG$BI_Clients_Exer31_12_N_Net <- ifelse(
  !is.na(DCF_PROG$BI_Clients_Exer31_12_N_Net),
  DCF_PROG$BI_Clients_Exer31_12_N_Net,
  0
)

# Calcul de la variation du bénéfice imposable par groupe NUM_IFU
DCF_PROG <- transform(
  DCF_PROG,
  var_BI = ave(BI_Clients_Exer31_12_N_Net, NUM_IFU, FUN = function(x) c(NA, diff(x)))
)

# Calcul de la variation du chiffre d'affaires hors taxes par groupe NUM_IFU
DCF_PROG <- transform(
  DCF_PROG,
  var_CAHT = ave(XB_CA_31_12_N_Net, NUM_IFU, FUN = function(x) c(NA, diff(x)))
)

# Initialisation de BD_TVA_Shiny si nécessaire
if (!exists("BD_TVA_Shiny")) {
  BD_TVA_Shiny <- DCF_PROG  # ou data.frame() si structure différente
}

# Calcul du ratio
BD_TVA_Shiny$RATIO_IND_37 <- with(DCF_PROG, ifelse(
  !is.na(var_CAHT) & var_CAHT != 0 &
    !is.na(var_BI) & var_BI != 0,
  var_BI / var_CAHT,
  0
))

# Calcul du risque
BD_TVA_Shiny$RISQUE_IND_37 <- with(DCF_PROG, ifelse(
  BD_TVA_Shiny$RATIO_IND_37 < 0 & var_BI < 0 & var_CAHT > 0,
  "rouge",
  "vert"
))



########## INDICATEUR 38
#Calcul du ratio
for (i in 1:nrow(DCF_PROG)){
  if(DCF_PROG$XB_CA_31_12_N_Net[i]!=0 & !is.na(DCF_PROG$XB_CA_31_12_N_Net[i]) & !is.na(DCF_PROG$IBENEF_EXIGIBLE[i])){
    DCF_PROG$RATIO_38[i]=DCF_PROG$IBENEF_EXIGIBLE[i]/DCF_PROG$XB_CA_31_12_N_Net[i]
  }
  else{
    DCF_PROG$RATIO_38[i]=0
  }
}

# Determination du risque
for (i in 1:nrow(DCF_PROG)){
  if(DCF_PROG$RATIO_38[i]<0 & !is.na(DCF_PROG$VAR_CA_XB[i]) & !is.na(DCF_PROG$var_BI[i]) & DCF_PROG$var_BI[i]<0 & DCF_PROG$VAR_CA_XB[i]>0){
    BD_TVA_Shiny$RISQUE_IND_38[i]="rouge"
  }
  else{
    BD_TVA_Shiny$RISQUE_IND_38[i]="vert"
  }
}


############## INDICATEUR 39 #######################3

DCF_PROG <-transform(DCF_PROG,VAR_DD= ave(DD_TtlDetFinRessAssim_Exer31_12_N_Net, NUM_IFU, FUN = function(x) c(0, diff(x))))
DCF_PROG <-transform(DCF_PROG,VAR_CF= ave(TM_TransfChargFin_31_12_N_Net, NUM_IFU, FUN = function(x) c(0, diff(x))))

#Calcul du ratio
for (i in 1:nrow(DCF_PROG)){
  if(DCF_PROG$CP_TtlCptauxPropRessAssim_Exer31_12_N_Net[i]!=0 & !is.na(DCF_PROG$CP_TtlCptauxPropRessAssim_Exer31_12_N_Net[i]) & !is.na(DCF_PROG$VAR_DD[i])){
    DCF_PROG$RATIO_39[i]=DCF_PROG$VAR_DD[i]/DCF_PROG$CP_TtlCptauxPropRessAssim_Exer31_12_N_Net[i]
  }
  else{
    DCF_PROG$RATIO_39[i]=0
  }
}

# Determination du risque
for (i in 1:nrow(DCF_PROG)){
  if(!is.na(DCF_PROG$RATIO_39[i]) & DCF_PROG$RATIO_39[i]<0 & !is.na(DCF_PROG$VAR_CF[i]) & DCF_PROG$VAR_CF[i]>0){
    BD_TVA_Shiny$RISQUE_IND_39[i]="rouge"
  }
  else{
    BD_TVA_Shiny$RISQUE_IND_39[i]="vert"
  }
}







############## INDICATEUR NUMERO 46 
# Calcul du ratio
if("CAFG" %in% names(DCF_PROG) && nrow(DCF_PROG) > 0){
  for(i in 1:nrow(DCF_PROG)){
    if(!is.na(DCF_PROG$CAFG[i]) & DCF_PROG$CAFG[i]!=0){
      BD_TVA_Shiny$RATIO_IND_46[i] <- DCF_PROG$DD_TtlDetFinRessAssim_Exer31_12_N_Net[i] / DCF_PROG$CAFG[i]
    } else {
      BD_TVA_Shiny$RATIO_IND_46[i] <- 0
    }
    
    if(BD_TVA_Shiny$RATIO_IND_46[i] > 4){
      BD_TVA_Shiny$RISQUE_IND_46[i] <- "rouge"
    } else {
      BD_TVA_Shiny$RISQUE_IND_46[i] <- "vert"
    }
  }
} else {
  message("La colonne CAFG est absente ou DCF_PROG est vide.")
}




############## INDICATEUR NUMERO 47 ###################

# Vérification que les colonnes nécessaires existent
if(all(c("MontantBesoin_Financement", "FondsRoulement_AnneeN") %in% names(DCF_PROG)) && nrow(DCF_PROG) > 0){
  
  # Initialisation des colonnes dans BD_TVA_Shiny
  BD_TVA_Shiny$RATIO_IND_47 <- numeric(nrow(DCF_PROG))
  BD_TVA_Shiny$RISQUE_IND_47 <- character(nrow(DCF_PROG))
  
  for(i in 1:nrow(DCF_PROG)){
    # Vérification du dénominateur
    if(!is.na(DCF_PROG$MontantBesoin_Financement[i]) && DCF_PROG$MontantBesoin_Financement[i] != 0){
      BD_TVA_Shiny$RATIO_IND_47[i] <- DCF_PROG$FondsRoulement_AnneeN[i] / DCF_PROG$MontantBesoin_Financement[i]
    } else {
      BD_TVA_Shiny$RATIO_IND_47[i] <- 0
    }
    
    # Utiliser la bonne table pour le ratio
    if(BD_TVA_Shiny$RATIO_IND_47[i] < 0.6){
      BD_TVA_Shiny$RISQUE_IND_47[i] <- "rouge"
    } else {
      BD_TVA_Shiny$RISQUE_IND_47[i] <- "vert"
    }
  }
  
} else {
  message("Colonnes manquantes ou DCF_PROG est vide.")
}



######################## INDICATEUR NUMERO 49 ###########################



# 1. Calcul du ratio (en pourcentage)
DCF_PROG$RATIO_49 <- ifelse(
  !is.na(DCF_PROG$CP_TtlCptauxPropRessAssim_Exer31_12_N_Net) & DCF_PROG$CP_TtlCptauxPropRessAssim_Exer31_12_N_Net != 0,
  (DCF_PROG$DD_TtlDetFinRessAssim_Exer31_12_N_Net * 100) / DCF_PROG$CP_TtlCptauxPropRessAssim_Exer31_12_N_Net,
  0
)

# 2. Calcul de la médiane du ratio par secteur et année
DCF_PROG <- DCF_PROG %>%
  group_by(CODE_SECT_ACT, ANNEE_FISCAL) %>%
  mutate(MEDIAN_RATIO_49 = median(RATIO_49, na.rm = TRUE)) %>%
  ungroup()

# 3. Initialisation du vecteur de sortie
BD_TVA_Shiny$RISQUE_IND_49 <- character(nrow(DCF_PROG))

# 4. Évaluation de l'indicateur avec protection contre les NA
for (i in 1:nrow(DCF_PROG)) {
  
  libelle <- DCF_PROG$LIBELLE_GR_SECT_ACT[i]
  ratio <- DCF_PROG$RATIO_49[i]
  median_ratio <- DCF_PROG$MEDIAN_RATIO_49[i]
  
  if (!is.na(ratio) && !is.na(median_ratio) && !is.na(libelle) && libelle == "MINIER") {
    BD_TVA_Shiny$RISQUE_IND_49[i] <- ifelse(ratio > median_ratio, "rouge", "vert")
    
  } else if (!is.na(libelle) && libelle == "MINIER") {
    BD_TVA_Shiny$RISQUE_IND_49[i] <- "Non disponible"
    
  } else {
    BD_TVA_Shiny$RISQUE_IND_49[i] <- "Non disponible"
  }
}





########################### INDICATEUR NUMERO 57 ###########################

# 1. Fonction pour calculer l'indicateur 57
IND_57 <- function(numerateur, denominateur, seuil) {
  if (!is.na(denominateur) && denominateur != 0 && !is.na(numerateur)) {
    indicateur <- numerateur / denominateur
  } else {
    indicateur <- 0
  }
  
  if (indicateur > seuil) {
    groupe <- "vert"
  } else {
    groupe <- "rouge"
  }
  
  return(groupe)
}

# 2. Initialisation de la colonne dans BD_TVA_Shiny
BD_TVA_Shiny$RISQUE_IND_57 <- character(nrow(DCF_PROG))

# 3. Boucle avec gestion des NA
for (i in 1:nrow(DCF_PROG)) {
  
  libelle <- DCF_PROG$LIBELLE_GR_SECT_ACT[i]
  num <- DCF_PROG$XE_RESULT_EXPL_31_12_N_Net[i]
  denom <- DCF_PROG$CP_TtlCptauxPropRessAssim_Exer31_12_N_Net[i]
  
  if (!is.na(libelle) && libelle == "MINIER") {
    if (!is.na(num) && !is.na(denom)) {
      BD_TVA_Shiny$RISQUE_IND_57[i] <- IND_57(num, denom, seuil = 5)
    } else {
      BD_TVA_Shiny$RISQUE_IND_57[i] <- "Non disponible"
    }
  } else if (!is.na(libelle)) {
    BD_TVA_Shiny$RISQUE_IND_57[i] <- "Non concerné"
  } else {
    BD_TVA_Shiny$RISQUE_IND_57[i] <- "Non disponible"
  }
}


################# INDICATEUR NUMERO 58 

# Calcul du ratio de l'indicatteur 58
DCF_PROG$RATIO_58<-(DCF_PROG$DD_TtlDetFinRessAssim_Exer31_12_N_Net*100)/DCF_PROG$CP_TtlCptauxPropRessAssim_Exer31_12_N_Net

#CALCUL DE L'INDICATEUR 
for (i in 1:nrow(DCF_PROG)) {
  if (!is.na(DCF_PROG$RATIO_58[i]) && !is.na(DCF_PROG$LIBELLE_GR_SECT_ACT[i]) && DCF_PROG$LIBELLE_GR_SECT_ACT[i] == "MINIER") {
    BD_TVA_Shiny$RISQUE_IND_58[i] <- ifelse(DCF_PROG$RATIO_58[i] < 1, "rouge", "vert")
  } else if (!is.na(DCF_PROG$LIBELLE_GR_SECT_ACT[i]) && DCF_PROG$LIBELLE_GR_SECT_ACT[i] != "MINIER") {
    BD_TVA_Shiny$RISQUE_IND_58[i] <- "Non concerné"
  } else {
    BD_TVA_Shiny$RISQUE_IND_58[i] <- "Non disponible"
  }
}



unique(BD_TVA_Shiny$RISQUE_IND_58)


# Exportation des risques

ELIGIBLE_IND1=subset(BD_TVA_Shiny,(BD_TVA_Shiny$ANNEE_FISCAL>2022)&(BD_TVA_Shiny$RISQUE_IND_15_B=='rouge')&(BD_TVA_Shiny$RISQUE_IND_15_A=='rouge')&(BD_TVA_Shiny$RISQUE_IND_1 %in% c("rouge","orange","jaune")))
length(unique(ELIGIBLE_IND1$NUM_IFU))          
max(ELIGIBLE_IND1$DATE_DERNIERE_AVIS)
max(ELIGIBLE_IND1$DATE_DERNIERE_VG)
max(ELIGIBLE_IND1$DATE_DERNIERE_VP)
unique(ELIGIBLE_IND1$ANNEE_FISCAL)


ELIGIBLE_IND2=subset(BD_TVA_Shiny,(BD_TVA_Shiny$ANNEE_FISCAL>2022)&(BD_TVA_Shiny$RISQUE_IND_15_B=='rouge')&(BD_TVA_Shiny$RISQUE_IND_15_A=='rouge')&(BD_TVA_Shiny$RISQUE_IND_2 %in% c("rouge","orange","jaune")))
length(unique(ELIGIBLE_IND2$NUM_IFU))               
max(ELIGIBLE_IND2$DATE_DERNIERE_AVIS)
max(ELIGIBLE_IND2$DATE_DERNIERE_VG)
max(ELIGIBLE_IND2$DATE_DERNIERE_VP)
unique(ELIGIBLE_IND2$ANNEE_FISCAL)




ELIGIBLE_IND4=subset(BD_TVA_Shiny,(BD_TVA_Shiny$ANNEE_FISCAL>2022)&(BD_TVA_Shiny$RISQUE_IND_15_B=='rouge')&(BD_TVA_Shiny$RISQUE_IND_15_A=='rouge')&(BD_TVA_Shiny$RISQUE_IND_4 %in% c("rouge","orange","jaune")))
length(unique(ELIGIBLE_IND4$NUM_IFU))                

max(ELIGIBLE_IND4$DATE_DERNIERE_AVIS)
max(ELIGIBLE_IND4$DATE_DERNIERE_VG)
max(ELIGIBLE_IND4$DATE_DERNIERE_VP)
unique(ELIGIBLE_IND4$ANNEE_FISCAL)


ELIGIBLE_IND5=subset(BD_TVA_Shiny,(BD_TVA_Shiny$ANNEE_FISCAL>2022)&(BD_TVA_Shiny$RISQUE_IND_15_B=='rouge')&(BD_TVA_Shiny$RISQUE_IND_15_A=='rouge')&(BD_TVA_Shiny$RISQUE_IND_5 %in% c("rouge","orange","jaune")))
length(unique(ELIGIBLE_IND5$NUM_IFU))                
max(ELIGIBLE_IND5$DATE_DERNIERE_AVIS)
max(ELIGIBLE_IND5$DATE_DERNIERE_VG)
max(ELIGIBLE_IND5$DATE_DERNIERE_VP)
unique(ELIGIBLE_IND5$ANNEE_FISCAL)


ELIGIBLE_IND8=subset(BD_TVA_Shiny,(BD_TVA_Shiny$ANNEE_FISCAL>2022)&(BD_TVA_Shiny$RISQUE_IND_15_B=='rouge')&(BD_TVA_Shiny$RISQUE_IND_15_A=='rouge')&(BD_TVA_Shiny$RISQUE_IND_8 %in% c("rouge","orange","jaune")))
length(unique(ELIGIBLE_IND8$NUM_IFU))                                                                                                             
max(ELIGIBLE_IND8$DATE_DERNIERE_AVIS)
max(ELIGIBLE_IND8$DATE_DERNIERE_VG)
max(ELIGIBLE_IND8$DATE_DERNIERE_VP)
unique(ELIGIBLE_IND8$ANNEE_FISCAL)

ELIGIBLE_IND9=subset(BD_TVA_Shiny,(BD_TVA_Shiny$ANNEE_FISCAL>2022)&(BD_TVA_Shiny$RISQUE_IND_15_B=='rouge')&(BD_TVA_Shiny$RISQUE_IND_15_A=='rouge')&(BD_TVA_Shiny$RISQUE_IND_9 %in% c("rouge","orange","jaune")))
length(unique(ELIGIBLE_IND9$NUM_IFU))


ELIGIBLE_IND10=subset(BD_TVA_Shiny,(BD_TVA_Shiny$ANNEE_FISCAL>2022)&(BD_TVA_Shiny$RISQUE_IND_15_B=='rouge')&(BD_TVA_Shiny$RISQUE_IND_15_A=='rouge')&(BD_TVA_Shiny$RISQUE_IND_10 %in% c("rouge","orange","jaune")))
length(unique(ELIGIBLE_IND10$NUM_IFU))                                                                                                              
max(ELIGIBLE_IND10$DATE_DERNIERE_AVIS)
max(ELIGIBLE_IND10$DATE_DERNIERE_VG)
max(ELIGIBLE_IND10$DATE_DERNIERE_VP)


ELIGIBLE_IND12=subset(BD_TVA_Shiny,(BD_TVA_Shiny$ANNEE_FISCAL>2022)&(BD_TVA_Shiny$RISQUE_IND_15_B=='rouge')&(BD_TVA_Shiny$RISQUE_IND_15_A=='rouge')&(BD_TVA_Shiny$RISQUE_IND_12 %in% c("rouge","orange","jaune")))
length(unique(ELIGIBLE_IND12$NUM_IFU))                  

max(ELIGIBLE_IND12$DATE_DERNIERE_AVIS)
max(ELIGIBLE_IND12$DATE_DERNIERE_VG)
max(ELIGIBLE_IND12$DATE_DERNIERE_VP)



ELIGIBLE_IND13=subset(BD_TVA_Shiny,(BD_TVA_Shiny$ANNEE_FISCAL>2022)&(BD_TVA_Shiny$RISQUE_IND_15_B=='rouge')&(BD_TVA_Shiny$RISQUE_IND_15_A=='rouge')&(BD_TVA_Shiny$RISQUE_IND_13 %in% c("rouge","orange","jaune")))
length(unique(ELIGIBLE_IND13$NUM_IFU))                 

max(ELIGIBLE_IND13$DATE_DERNIERE_AVIS)
max(ELIGIBLE_IND13$DATE_DERNIERE_VG)
max(ELIGIBLE_IND13$DATE_DERNIERE_VP)



ELIGIBLE_IND14=subset(BD_TVA_Shiny,(BD_TVA_Shiny$ANNEE_FISCAL>2022)&(BD_TVA_Shiny$RISQUE_IND_15_B=='rouge')&(BD_TVA_Shiny$RISQUE_IND_15_A=='rouge')&(BD_TVA_Shiny$RISQUE_IND_14 %in% c("rouge","orange","jaune")))
length(unique(ELIGIBLE_IND14$NUM_IFU))   
length(unique(ELIGIBLE_IND14$NUM_IFU)) 

ELIGIBLE_IND16=subset(BD_TVA_Shiny,(BD_TVA_Shiny$ANNEE_FISCAL>2022)&(BD_TVA_Shiny$RISQUE_IND_15_B=='rouge')&(BD_TVA_Shiny$RISQUE_IND_15_A=='rouge')&(BD_TVA_Shiny$RISQUE_IND_16 %in% c("rouge","orange","jaune")))
length(unique(ELIGIBLE_IND16$NUM_IFU))                 


ELIGIBLE_IND20=subset(BD_TVA_Shiny,(BD_TVA_Shiny$ANNEE_FISCAL>2022)&(BD_TVA_Shiny$RISQUE_IND_15_B=='rouge')&(BD_TVA_Shiny$RISQUE_IND_15_A=='rouge')&(BD_TVA_Shiny$RISQUE_IND_20 %in% c("rouge","orange","jaune")))
length(unique(ELIGIBLE_IND20$NUM_IFU))                 


ELIGIBLE_IND21=subset(BD_TVA_Shiny,(BD_TVA_Shiny$ANNEE_FISCAL>2021)&(BD_TVA_Shiny$RISQUE_IND_15_B=='rouge')&(BD_TVA_Shiny$RISQUE_IND_15_A=='rouge')&(BD_TVA_Shiny$RISQUE_IND_21 %in% c("rouge","orange","jaune")))
length(unique(ELIGIBLE_IND21$NUM_IFU)) 


ELIGIBLE_IND23=subset(BD_TVA_Shiny,(BD_TVA_Shiny$ANNEE_FISCAL>2022)&(BD_TVA_Shiny$RISQUE_IND_15_B=='rouge')&(BD_TVA_Shiny$RISQUE_IND_15_A=='rouge')&(BD_TVA_Shiny$RISQUE_IND_23 %in% c("rouge","orange","jaune")))
length(unique(ELIGIBLE_IND23$NUM_IFU)) 



ELIGIBLE_IND24=subset(BD_TVA_Shiny,(BD_TVA_Shiny$ANNEE_FISCAL>2021)&(BD_TVA_Shiny$RISQUE_IND_15_B=='rouge')&(BD_TVA_Shiny$RISQUE_IND_15_A=='rouge')&(BD_TVA_Shiny$RISQUE_IND_24 %in% c("rouge","orange","jaune")))
length(unique(ELIGIBLE_IND24$NUM_IFU))


ELIGIBLE_IND25=subset(BD_TVA_Shiny,(BD_TVA_Shiny$ANNEE_FISCAL>2022)&(BD_TVA_Shiny$RISQUE_IND_15_B=='rouge')&(BD_TVA_Shiny$RISQUE_IND_15_A=='rouge')&(BD_TVA_Shiny$RISQUE_IND_25 %in% c("rouge","orange","jaune")))
length(unique(ELIGIBLE_IND25$NUM_IFU))


ELIGIBLE_IND26=subset(BD_TVA_Shiny,(BD_TVA_Shiny$ANNEE_FISCAL>2022)&(BD_TVA_Shiny$RISQUE_IND_15_B=='rouge')&(BD_TVA_Shiny$RISQUE_IND_15_A=='rouge')&(BD_TVA_Shiny$RISQUE_IND_26 %in% c("rouge","orange","jaune")))
length(unique(ELIGIBLE_IND26$NUM_IFU))


ELIGIBLE_IND27=subset(BD_TVA_Shiny,(BD_TVA_Shiny$ANNEE_FISCAL>2022)&(BD_TVA_Shiny$RISQUE_IND_15_B=='rouge')&(BD_TVA_Shiny$RISQUE_IND_15_A=='rouge')&(BD_TVA_Shiny$RISQUE_IND_27 %in% c("rouge","orange","jaune")))
length(unique(ELIGIBLE_IND27$NUM_IFU))



ELIGIBLE_IND29=subset(BD_TVA_Shiny,(BD_TVA_Shiny$ANNEE_FISCAL>2022)&(BD_TVA_Shiny$RISQUE_IND_15_B=='rouge')&(BD_TVA_Shiny$RISQUE_IND_15_A=='rouge')&(BD_TVA_Shiny$RISQUE_IND_29 %in% c("rouge","orange","jaune")))
length(unique(ELIGIBLE_IND29$NUM_IFU))


ELIGIBLE_IND30=subset(BD_TVA_Shiny,(BD_TVA_Shiny$ANNEE_FISCAL>2022)&(BD_TVA_Shiny$RISQUE_IND_15_B=='rouge')&(BD_TVA_Shiny$RISQUE_IND_15_A=='rouge')&(BD_TVA_Shiny$RISQUE_IND_30 %in% c("rouge","orange","jaune")))
length(unique(ELIGIBLE_IND30$NUM_IFU))


ELIGIBLE_IND32=subset(BD_TVA_Shiny,(BD_TVA_Shiny$ANNEE_FISCAL>2022)&(BD_TVA_Shiny$RISQUE_IND_15_B=='rouge')&(BD_TVA_Shiny$RISQUE_IND_15_A=='rouge')&(BD_TVA_Shiny$RISQUE_IND_32 %in% c("rouge","orange","jaune")))
length(unique(ELIGIBLE_IND32$NUM_IFU))


ELIGIBLE_IND33=subset(BD_TVA_Shiny,(BD_TVA_Shiny$ANNEE_FISCAL>2022)&(BD_TVA_Shiny$RISQUE_IND_15_B=='rouge')&(BD_TVA_Shiny$RISQUE_IND_15_A=='rouge')&(BD_TVA_Shiny$RISQUE_IND_33 %in% c("rouge","orange","jaune")))
length(unique(ELIGIBLE_IND33$NUM_IFU))
ELIGIBLE_IND34=subset(BD_TVA_Shiny,(BD_TVA_Shiny$ANNEE_FISCAL>2022)&(BD_TVA_Shiny$RISQUE_IND_15_B=='rouge')&(BD_TVA_Shiny$RISQUE_IND_15_A=='rouge')&(BD_TVA_Shiny$RISQUE_IND_34 %in% c("rouge","orange","jaune")))
length(unique(ELIGIBLE_IND34$NUM_IFU))

#Global=rbind(ELIGIBLE_IND1,ELIGIBLE_IND2,ELIGIBLE_IND4,ELIGIBLE_IND5,ELIGIBLE_IND8,ELIGIBLE_IND9,ELIGIBLE_IND10,ELIGIBLE_IND12,ELIGIBLE_IND13,ELIGIBLE_IND14,ELIGIBLE_IND16,ELIGIBLE_IND20,ELIGIBLE_IND21,ELIGIBLE_IND23,ELIGIBLE_IND24,ELIGIBLE_IND25,ELIGIBLE_IND26,ELIGIBLE_IND27,ELIGIBLE_IND29,ELIGIBLE_IND30,ELIGIBLE_IND32,ELIGIBLE_IND33,ELIGIBLE_IND34)
#length(unique(Global$NUM_IFU)) 
library(dplyr)

Global <- bind_rows(
  ELIGIBLE_IND1, ELIGIBLE_IND2, ELIGIBLE_IND4, ELIGIBLE_IND5, ELIGIBLE_IND8, ELIGIBLE_IND9,
  ELIGIBLE_IND10, ELIGIBLE_IND12, ELIGIBLE_IND13, ELIGIBLE_IND14, ELIGIBLE_IND16, ELIGIBLE_IND20,
  ELIGIBLE_IND21, ELIGIBLE_IND23, ELIGIBLE_IND24, ELIGIBLE_IND25, ELIGIBLE_IND26, ELIGIBLE_IND27,
  ELIGIBLE_IND29, ELIGIBLE_IND30, ELIGIBLE_IND32, ELIGIBLE_IND33, ELIGIBLE_IND34
)

