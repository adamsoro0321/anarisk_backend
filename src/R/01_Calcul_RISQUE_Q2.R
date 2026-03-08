library(stringr)
library(gsubfn)
library(readxl)
library(dplyr)
library(odbc)
library(RPostgreSQL)
library(RPostgres)
library(lubridate)
library(RODBC)
library(reshape2)
library(data.table)
library(readr)
library(openxlsx)
setwd("Z:/PV_Q1_2024")

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
DCF_PROG_total=DCF_PROG_csb
NON_EIGIBLE <- read_excel("NON_EIGIBLE.xlsx")
LISTE_NON_EIGIBLE=NON_EIGIBLE$NUM_IFU

DCF_PROG=subset(DCF_PROG_total, !(DCF_PROG_total$NUM_IFU %in% LISTE_NON_EIGIBLE))

BD_TVA_Shiny<-DCF_PROG[,c("NUM_IFU","NOM_MINEFID","ETAT","CODE_SECT_ACT","CODE_REG_FISC", "STRUCTURES" ,"ANNEE_FISCAL","DATE_DERNIERE_VG","DATE_DERNIERE_VP","DATE_DERNIERE_AVIS")]

############## INDICATEUR NUMERO 15_A 


# Identifier les entreprises avec une date de programmation et une date d'avis de vérification supérieures à 3 ans
BD_TVA_Shiny$RISQUE_IND_15_A <- ifelse(((BD_TVA_Shiny$DATE_DERNIERE_VG <'2021-12-31') & (BD_TVA_Shiny$DATE_DERNIERE_AVIS <'2021-12-31')), "rouge", "vert")

BD_TVA_Shiny$RISQUE_IND_15_B <- ifelse((BD_TVA_Shiny$DATE_DERNIERE_VP <'2021-12-31') & (BD_TVA_Shiny$DATE_DERNIERE_AVIS <'2021-12-31'), "rouge", "vert")

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



################ Calcul sur le Q2 ########################################################


TVA_IND1<-function(criticite,numerateur,denominateur,seuil,coeff,x1,x2,x3,x4){
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
  if(!is.na(DCF_PROG$MONTANT_TVA_NET_A_PAYER_25[i]) & !is.na(DCF_PROG$Fourn_TVA_DEDUCTIBLE_AN[i])){
    BD_TVA_Shiny$RISQUE_IND_1[i]=TVA_IND1(5,DCF_PROG$MONTANT_TVA_NET_A_PAYER_25[i],DCF_PROG$Fourn_TVA_DEDUCTIBLE_AN[i],0.2,0.8,500000,5000000,20000000,100000000)[2]
    BD_TVA_Shiny$GAP_IND_1[i]=TVA_IND1(5,DCF_PROG$MONTANT_TVA_NET_A_PAYER_25[i],DCF_PROG$Fourn_TVA_DEDUCTIBLE_AN[i],0.2,0.8,500000,5000000,20000000,100000000)[1]
    BD_TVA_Shiny$SCORE_IND_1[i]=TVA_IND1(5,DCF_PROG$MONTANT_TVA_NET_A_PAYER_25[i],DCF_PROG$Fourn_TVA_DEDUCTIBLE_AN[i],0.2,0.8,500000,5000000,20000000,100000000)[3]
    
  }
  else{
    BD_TVA_Shiny$RISQUE_IND_1[i]="Non disponible"
    BD_TVA_Shiny$GAP_IND_1[i]=0
    BD_TVA_Shiny$SCORE_IND_1[i]=0
  }
  #BD_TVA_1_AG$DESIGNATION[i]="RISQUE DE DELIVRANCE DE FAUSSE FACTURE"
}

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

TVA_IND8<-function(criticite,numerateur,denominateur,seuil,coeff,x1,x2,x3,x4){
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
  if(!is.na(DCF_PROG$MONTANT_DECLARE[i]) & !is.na(DCF_PROG$Fourn_TVA_DEDUCTIBLE_AN[i])){
    BD_TVA_Shiny$RISQUE_IND_8[i]=TVA_IND8(4,DCF_PROG$MONTANT_DECLARE[i],DCF_PROG$Fourn_TVA_DEDUCTIBLE_AN[i],0.95,0.75,500000,5000000,20000000,100000000)[2]
    BD_TVA_Shiny$GAP_IND_8[i]=TVA_IND8(4,DCF_PROG$MONTANT_DECLARE[i],DCF_PROG$Fourn_TVA_DEDUCTIBLE_AN[i],0.95,0.75,500000,5000000,20000000,100000000)[1]
    BD_TVA_Shiny$SCORE_IND_8[i]=TVA_IND8(4,DCF_PROG$MONTANT_DECLARE[i],DCF_PROG$Fourn_TVA_DEDUCTIBLE_AN[i],0.95,0.75,500000,5000000,20000000,100000000)[3]

  }

  else{
    BD_TVA_Shiny$RISQUE_IND_8[i]="Non disponible"
    BD_TVA_Shiny$GAP_IND_8[i]=0
    BD_TVA_Shiny$SCORE_IND_8[i]=0
  }
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
# Fonction pour le calcul DU RISQUE DE DELIVRANCE DE FAUSSE FACTURE

TVA_IND14<-function(ca){
  
  if(ca!=0){
    groupe="vert"
  }
  else 
  {
    groupe="rouge"
  }
  return(groupe)
}

#CALCUL DE L'INDICATEUR DU RISQUE DE DELIVRANCE DE FAUSSE FACTURE, IND_1
for (i in 1:nrow(DCF_PROG)){
  if((!is.na(DCF_PROG$MONTANT_TOTAL_LA_BRUTE_15[i])) & (DCF_PROG$MONTANT_TOTAL_LA_BRUTE_15[i]==0) &(DCF_PROG$CODE_REG_FISC[i] %in% c("RN","ND"))){
    BD_TVA_Shiny$RISQUE_IND_14[i]="rouge"
  }
  else{
    BD_TVA_Shiny$RISQUE_IND_14[i]="vert"
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

ELIGIBLE_IND1=subset(BD_TVA_Shiny,(BD_TVA_Shiny$ANNEE_FISCAL>2021)&(BD_TVA_Shiny$RISQUE_IND_15_B=='rouge')&(BD_TVA_Shiny$RISQUE_IND_15_A=='rouge')&(BD_TVA_Shiny$RISQUE_IND_1 %in% c("rouge","orange","jaune")))

length(unique(ELIGIBLE_IND1$NUM_IFU))          
max(ELIGIBLE_IND1$DATE_DERNIERE_AVIS)
max(ELIGIBLE_IND1$DATE_DERNIERE_VG)
max(ELIGIBLE_IND1$DATE_DERNIERE_VP)
unique(ELIGIBLE_IND1$ANNEE_FISCAL)


ELIGIBLE_IND2=subset(BD_TVA_Shiny,(BD_TVA_Shiny$ANNEE_FISCAL>2021)&(BD_TVA_Shiny$RISQUE_IND_15_B=='rouge')&(BD_TVA_Shiny$RISQUE_IND_15_A=='rouge')&(BD_TVA_Shiny$RISQUE_IND_2 %in% c("rouge","orange","jaune")))

length(unique(ELIGIBLE_IND2$NUM_IFU))               
max(ELIGIBLE_IND2$DATE_DERNIERE_AVIS)
max(ELIGIBLE_IND2$DATE_DERNIERE_VG)
max(ELIGIBLE_IND2$DATE_DERNIERE_VP)
unique(ELIGIBLE_IND2$ANNEE_FISCAL)




ELIGIBLE_IND4=subset(BD_TVA_Shiny,(BD_TVA_Shiny$ANNEE_FISCAL>2021)&(BD_TVA_Shiny$RISQUE_IND_15_B=='rouge')&(BD_TVA_Shiny$RISQUE_IND_15_A=='rouge')&(BD_TVA_Shiny$RISQUE_IND_4 %in% c("rouge","orange","jaune")))
length(unique(ELIGIBLE_IND4$NUM_IFU))                

max(ELIGIBLE_IND4$DATE_DERNIERE_AVIS)
max(ELIGIBLE_IND4$DATE_DERNIERE_VG)
max(ELIGIBLE_IND4$DATE_DERNIERE_VP)
unique(ELIGIBLE_IND4$ANNEE_FISCAL)


ELIGIBLE_IND5=subset(BD_TVA_Shiny,(BD_TVA_Shiny$ANNEE_FISCAL>2021)&(BD_TVA_Shiny$RISQUE_IND_15_B=='rouge')&(BD_TVA_Shiny$RISQUE_IND_15_A=='rouge')&(BD_TVA_Shiny$RISQUE_IND_5 %in% c("rouge","orange","jaune")))
length(unique(ELIGIBLE_IND5$NUM_IFU))                
max(ELIGIBLE_IND5$DATE_DERNIERE_AVIS)
max(ELIGIBLE_IND5$DATE_DERNIERE_VG)
max(ELIGIBLE_IND5$DATE_DERNIERE_VP)
unique(ELIGIBLE_IND5$ANNEE_FISCAL)


ELIGIBLE_IND8=subset(BD_TVA_Shiny,(BD_TVA_Shiny$ANNEE_FISCAL>2021)&(BD_TVA_Shiny$RISQUE_IND_15_B=='rouge')&(BD_TVA_Shiny$RISQUE_IND_15_A=='rouge')&(BD_TVA_Shiny$RISQUE_IND_8 %in% c("rouge","orange","jaune")))
length(unique(ELIGIBLE_IND8$NUM_IFU))                                                                                                             
max(ELIGIBLE_IND8$DATE_DERNIERE_AVIS)
max(ELIGIBLE_IND8$DATE_DERNIERE_VG)
max(ELIGIBLE_IND8$DATE_DERNIERE_VP)
unique(ELIGIBLE_IND8$ANNEE_FISCAL)

ELIGIBLE_IND9=subset(BD_TVA_Shiny,(BD_TVA_Shiny$ANNEE_FISCAL>2021)&(BD_TVA_Shiny$RISQUE_IND_15_B=='rouge')&(BD_TVA_Shiny$RISQUE_IND_15_A=='rouge')&(BD_TVA_Shiny$RISQUE_IND_9 %in% c("rouge","orange","jaune")))
length(unique(ELIGIBLE_IND9$NUM_IFU))


ELIGIBLE_IND10=subset(BD_TVA_Shiny,(BD_TVA_Shiny$ANNEE_FISCAL>2021)&(BD_TVA_Shiny$RISQUE_IND_15_B=='rouge')&(BD_TVA_Shiny$RISQUE_IND_15_A=='rouge')&(BD_TVA_Shiny$RISQUE_IND_10 %in% c("rouge","orange","jaune")))
length(unique(ELIGIBLE_IND10$NUM_IFU))                                                                                                              
max(ELIGIBLE_IND10$DATE_DERNIERE_AVIS)
max(ELIGIBLE_IND10$DATE_DERNIERE_VG)
max(ELIGIBLE_IND10$DATE_DERNIERE_VP)


ELIGIBLE_IND12=subset(BD_TVA_Shiny,(BD_TVA_Shiny$ANNEE_FISCAL>2021)&(BD_TVA_Shiny$RISQUE_IND_15_B=='rouge')&(BD_TVA_Shiny$RISQUE_IND_15_A=='rouge')&(BD_TVA_Shiny$RISQUE_IND_12 %in% c("rouge","orange","jaune")))
length(unique(ELIGIBLE_IND12$NUM_IFU))                  

max(ELIGIBLE_IND12$DATE_DERNIERE_AVIS)
max(ELIGIBLE_IND12$DATE_DERNIERE_VG)
max(ELIGIBLE_IND12$DATE_DERNIERE_VP)



ELIGIBLE_IND13=subset(BD_TVA_Shiny,(BD_TVA_Shiny$ANNEE_FISCAL>2021)&(BD_TVA_Shiny$RISQUE_IND_15_B=='rouge')&(BD_TVA_Shiny$RISQUE_IND_15_A=='rouge')&(BD_TVA_Shiny$RISQUE_IND_13 %in% c("rouge","orange","jaune")))
length(unique(ELIGIBLE_IND13$NUM_IFU))                 

max(ELIGIBLE_IND13$DATE_DERNIERE_AVIS)
max(ELIGIBLE_IND13$DATE_DERNIERE_VG)
max(ELIGIBLE_IND13$DATE_DERNIERE_VP)



ELIGIBLE_IND14=subset(BD_TVA_Shiny,(BD_TVA_Shiny$ANNEE_FISCAL>2021)&(BD_TVA_Shiny$RISQUE_IND_15_B=='rouge')&(BD_TVA_Shiny$RISQUE_IND_15_A=='rouge')&(BD_TVA_Shiny$RISQUE_IND_14 %in% c("rouge","orange","jaune")))
length(unique(ELIGIBLE_IND14$NUM_IFU))   
length(unique(ELIGIBLE_IND14$NUM_IFU)) 

ELIGIBLE_IND16=subset(BD_TVA_Shiny,(BD_TVA_Shiny$ANNEE_FISCAL>2021)&(BD_TVA_Shiny$RISQUE_IND_15_B=='rouge')&(BD_TVA_Shiny$RISQUE_IND_15_A=='rouge')&(BD_TVA_Shiny$RISQUE_IND_16 %in% c("rouge","orange","jaune")))
length(unique(ELIGIBLE_IND16$NUM_IFU))                 


ELIGIBLE_IND20=subset(BD_TVA_Shiny,(BD_TVA_Shiny$ANNEE_FISCAL>2021)&(BD_TVA_Shiny$RISQUE_IND_15_B=='rouge')&(BD_TVA_Shiny$RISQUE_IND_15_A=='rouge')&(BD_TVA_Shiny$RISQUE_IND_20 %in% c("rouge","orange","jaune")))
length(unique(ELIGIBLE_IND20$NUM_IFU))                 


ELIGIBLE_IND21=subset(BD_TVA_Shiny,(BD_TVA_Shiny$ANNEE_FISCAL>2021)&(BD_TVA_Shiny$RISQUE_IND_15_B=='rouge')&(BD_TVA_Shiny$RISQUE_IND_15_A=='rouge')&(BD_TVA_Shiny$RISQUE_IND_21 %in% c("rouge","orange","jaune")))
length(unique(ELIGIBLE_IND21$NUM_IFU)) 


ELIGIBLE_IND23=subset(BD_TVA_Shiny,(BD_TVA_Shiny$ANNEE_FISCAL>2021)&(BD_TVA_Shiny$RISQUE_IND_15_B=='rouge')&(BD_TVA_Shiny$RISQUE_IND_15_A=='rouge')&(BD_TVA_Shiny$RISQUE_IND_23 %in% c("rouge","orange","jaune")))
length(unique(ELIGIBLE_IND23$NUM_IFU)) 



ELIGIBLE_IND24=subset(BD_TVA_Shiny,(BD_TVA_Shiny$ANNEE_FISCAL>2021)&(BD_TVA_Shiny$RISQUE_IND_15_B=='rouge')&(BD_TVA_Shiny$RISQUE_IND_15_A=='rouge')&(BD_TVA_Shiny$RISQUE_IND_24 %in% c("rouge","orange","jaune")))
length(unique(ELIGIBLE_IND24$NUM_IFU))


ELIGIBLE_IND25=subset(BD_TVA_Shiny,(BD_TVA_Shiny$ANNEE_FISCAL>2021)&(BD_TVA_Shiny$RISQUE_IND_15_B=='rouge')&(BD_TVA_Shiny$RISQUE_IND_15_A=='rouge')&(BD_TVA_Shiny$RISQUE_IND_25 %in% c("rouge","orange","jaune")))
length(unique(ELIGIBLE_IND25$NUM_IFU))


ELIGIBLE_IND26=subset(BD_TVA_Shiny,(BD_TVA_Shiny$ANNEE_FISCAL>2021)&(BD_TVA_Shiny$RISQUE_IND_15_B=='rouge')&(BD_TVA_Shiny$RISQUE_IND_15_A=='rouge')&(BD_TVA_Shiny$RISQUE_IND_26 %in% c("rouge","orange","jaune")))
length(unique(ELIGIBLE_IND26$NUM_IFU))


ELIGIBLE_IND27=subset(BD_TVA_Shiny,(BD_TVA_Shiny$ANNEE_FISCAL>2021)&(BD_TVA_Shiny$RISQUE_IND_15_B=='rouge')&(BD_TVA_Shiny$RISQUE_IND_15_A=='rouge')&(BD_TVA_Shiny$RISQUE_IND_27 %in% c("rouge","orange","jaune")))
length(unique(ELIGIBLE_IND27$NUM_IFU))



ELIGIBLE_IND29=subset(BD_TVA_Shiny,(BD_TVA_Shiny$ANNEE_FISCAL>2021)&(BD_TVA_Shiny$RISQUE_IND_15_B=='rouge')&(BD_TVA_Shiny$RISQUE_IND_15_A=='rouge')&(BD_TVA_Shiny$RISQUE_IND_29 %in% c("rouge","orange","jaune")))
length(unique(ELIGIBLE_IND29$NUM_IFU))


ELIGIBLE_IND30=subset(BD_TVA_Shiny,(BD_TVA_Shiny$ANNEE_FISCAL>2021)&(BD_TVA_Shiny$RISQUE_IND_15_B=='rouge')&(BD_TVA_Shiny$RISQUE_IND_15_A=='rouge')&(BD_TVA_Shiny$RISQUE_IND_30 %in% c("rouge","orange","jaune")))
length(unique(ELIGIBLE_IND30$NUM_IFU))


ELIGIBLE_IND32=subset(BD_TVA_Shiny,(BD_TVA_Shiny$ANNEE_FISCAL>2021)&(BD_TVA_Shiny$RISQUE_IND_15_B=='rouge')&(BD_TVA_Shiny$RISQUE_IND_15_A=='rouge')&(BD_TVA_Shiny$RISQUE_IND_32 %in% c("rouge","orange","jaune")))
length(unique(ELIGIBLE_IND32$NUM_IFU))


ELIGIBLE_IND33=subset(BD_TVA_Shiny,(BD_TVA_Shiny$ANNEE_FISCAL>2021)&(BD_TVA_Shiny$RISQUE_IND_15_B=='rouge')&(BD_TVA_Shiny$RISQUE_IND_15_A=='rouge')&(BD_TVA_Shiny$RISQUE_IND_33 %in% c("rouge","orange","jaune")))
length(unique(ELIGIBLE_IND33$NUM_IFU))
ELIGIBLE_IND34=subset(BD_TVA_Shiny,(BD_TVA_Shiny$ANNEE_FISCAL>2021)&(BD_TVA_Shiny$RISQUE_IND_15_B=='rouge')&(BD_TVA_Shiny$RISQUE_IND_15_A=='rouge')&(BD_TVA_Shiny$RISQUE_IND_34 %in% c("rouge","orange","jaune")))
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




#################Exportation des classeurs du T1 ###############################################################################################

#################Exportation des classeurs du T1 ###############################################################################################

# Exportation des feuilles

QUANTUM_2<- Q2
FICHE_PISTES_INVESTIGATION <- read_excel("FICHE_PISTES_INVESTIGATION.xlsx")
SUIVI <- read_excel("SUIVI.xlsx")
META_DONNEES <- read_excel("META_DONNEES.xlsx")

NomenclatureSH <- read_excel("NomenclatureSH.xlsx")
names(SUIVI)=c("NOTIF_INITIAL_REF","NOTIF_INITIAL_IMPOT","NOTIF_INITIAL_DS","NOTIF_INITIAL_PEN","NOTIF_FINAL_REF","NOTIF_FINAL_DS","NOTIF_FINAL_PEN","NOTIF_FINAL_PEN","TITRE_EMIS_REF","TITRE_EMIS_DS"  ,"TITRE_EMIS_PEN", "TITRE_EMIS_PEN","OBSERVATION")


PROG1=QUANTUM_2[,c("STRUCTURES","NUM_IFU","BRIGADES","ACTIVITES","TYPE_ CONTROLE")]


PROG1=Global

##IFU_conex <- odbcConnect("IFU_con", uid="toussaintifu", pwd="toussaint2025")
con <- dbConnect(odbc::odbc(),
                 Driver = "Oracle in XE",               # nom du driver exact
                 Dbq    = "ifubase.dgi.bf:1521/IFU3",  # chaîne Easy Connect (host:port/service_name)
                 UID    = "toussaintifu",
                 PWD    = "toussaint2025") 

ReqIFU="select  DCIS.LIBELLE structures,numeroifu NUM_IFU,IFU3.CONTRIBUABLES.LATITUDE, IFU3.CONTRIBUABLES.LONGITUDE,   IFU3.NAEMAACTIVITES.LIBELLE ACTIVITES_IFU
from ifu3.contribuables, ifu3.dcis,IFU3.FORMEJURIDIQUES, IFU3.REGIMES, IFU3.NAEMAACTIVITES
where
ifu3.CONTRIBUABLES.IDDCI = ifu3.DCIS.IDDCI
and  IFU3.REGIMES.IDREGIME=ifu3.CONTRIBUABLES.IDREGIME
AND IFU3.CONTRIBUABLES.IDFORME=IFU3.FORMEJURIDIQUES.IDFORME
AND IFU3.CONTRIBUABLES.IDNAEMA= IFU3.NAEMAACTIVITES.IDACTIVITE;
 "
IFU<- dbGetQuery(con, ReqIFU)



PROG2=left_join(PROG1, IFU, by="NUM_IFU")

PROG3=left_join(BD_TVA_Shiny,PROG2, by="NUM_IFU")


PROG3=left_join(BD_TVA_Shiny,PROG2)
donnees<-PROG3


# donnees=head(PROG3)


PROGRAMMATIONS$NUM_IFU



#donneesQ1=subset(donnees,(donnees$NUM_IFU %in% listeQ1))

NOneligible=subset(donnees,!(donnees$NUM_IFU %in%  listeNonEl))
length(unique(NOneligible$NUM_IFU))

wb1 <- createWorkbook()
# # Ajouter une feuille de calcul au classeur
# addWorksheet(wb1, sheetName = "donneesQ1")
# writeData(wb1, sheet = "donneesQ1", x = donneesQ1)
# 
# addWorksheet(wb1, sheetName = "NonEligibles")
# writeData(wb1, sheet = "NonEligibles", x = NOneligible)

addWorksheet(wb1, sheetName = "nd")

writeData(wb1, sheet = "nd", x = PROG2)

saveWorkbook(wb1, "ND.xlsx", overwrite = TRUE)


#donnees <- read_excel("Z:/PV_Q1_2024/Base_ProgQ2.xlsx")



donnees=subset(donnees,(donnees$ANNEE_FISCAL %in% c(2022,2023,2024))& !(donnees$NUM_IFU %in% listeQ1))

donnees=subset(donnees,(donnees$ANNEE_FISCAL %in% c(2022,2023,2024)))


# donnees=head(donnees,5)
contribuables=paste0(donnees$STRUCTURES,"_IFU",donnees$NUM_IFU,"_AN",donnees$ANNEE_FISCAL)
head(contribuables)

donnees$STRUCTURES=str_replace(donnees$STRUCTURES, pattern = "/", replacement = "_")
donnees$STRUCTURES=str_replace(donnees$STRUCTURES, pattern = "-", replacement = "_")

donnees$STRUCTURES.y=str_replace(donnees$STRUCTURES.y, pattern = "/", replacement = "_")
donnees$STRUCTURES.y=str_replace(donnees$STRUCTURES.y, pattern = "-", replacement = "_")

donnees$STRUCTURES.x=str_replace(donnees$STRUCTURES.x, pattern = "/", replacement = "_")
donnees$STRUCTURES.x=str_replace(donnees$STRUCTURES.x, pattern = "-", replacement = "_")



contribuables=str_replace(contribuables, pattern = "/", replacement = "_")
contribuables=str_replace(contribuables, pattern = "-", replacement = "_")
donnees$contribuables=contribuables

##channel <- odbcConnect("ods", uid="ODSDI1", pwd="odsdi1")
channel <- dbConnect(odbc::odbc(),
                     Driver = "Oracle in XE",               # nom du driver exact
                     Dbq    = "10.3.1.32:1521/SIDDGI",  # chaîne Easy Connect (host:port/service_name)
                     UID    = "ODSDI1",
                     PWD    = "odsdi1")
# Charger les données depuis Oracle
Req_DONNEES_DGD <- "SELECT * FROM sid_dgd_CPF WHERE EXTRACT(YEAR FROM DATE_LIQUIDATION) in (2022,2023,2024);"
DONNEES_DGD <- dbGetQuery(channel, Req_DONNEES_DGD)

# Ajouter colonne AN pour l'année
DONNEES_DGD$AN <- substr(DONNEES_DGD$DATE_LIQUIDATION, 1, 4)
DGD <- DONNEES_DGD

# Réduction aux colonnes nécessaires
DGD <- DGD[, c('CODE_BUREAU', 'LIB_BUREAU', 'NUM_LIQUIDATION', 'DATE_LIQUIDATION',
               'NUM_ARTICLE', 'NOMENCLATURE8', 'LIBELLE', 'PAYS_ORIGINE',
               'PAYS_DESTINATION1', 'PAYS_DESTINATION_FINALE', 'FLUX', 'REGIME',
               'TYPE_DECLARATION', 'IFU', 'ENTREPRISE', 'LIBELLE_BUREAU_FRONTIERE',
               'PDS_NET', 'PDS_BRT', 'QUANTITE', 'CAF', 'FOB', 'DATE_ENREGISTREMENT',
               'COMPTE', 'LIBELLE_MODE_TRANSPORT', 'NUM_DECLARATION', 'DECLARANT',
               'LIB_DECLARANT', 'DD', 'CSE', 'RS', 'TSB', 'TSC', 'TST', 'TIC', 'TVA',
               'PCS', 'PC', 'TPP', 'CPV', 'IDR', 'IFU_DECLARANT', 'ACOMPTE', 'RSP',
               'ETAT', 'ANNULEE', 'TEP', 'TPC', 'TVT', 'AN')]

# Nettoyage et encodage UTF-8
donnees <- donnees[ , -c(8:10)]
donnees <- head(donnees, 3)
donnees[] <- lapply(donnees, function(col) {
  if (is.character(col)) stringi::stri_enc_toutf8(col) else col
})

# Boucle principale sur les contribuables
for (contribuable in contribuables) {
  
  contribuable <- stringi::stri_enc_toutf8(as.character(contribuable))
  contribuable <- gsub("[^A-Za-z0-9_]", "_", contribuable)
  
  entreprise_data <- subset(donnees, donnees$contribuables == contribuable)
  entreprise_data[] <- lapply(entreprise_data, function(col) {
    if (is.character(col)) stringi::stri_enc_toutf8(col) else col
  })
  
  # Création du workbook
  wb <- createWorkbook()
  
  ## Fiche Contribuable
  addWorksheet(wb, "Fiche_Contribuable")
  writeData(wb, "Fiche_Contribuable", x = contribuable, startCol = 1, startRow = 1)
  setColWidths(wb, "Fiche_Contribuable", cols = 2:4, widths = 40)
  
  ## Analyse Risque
  addWorksheet(wb, "Analyse_Risque")
  writeData(wb, "Analyse_Risque", x = contribuable, startCol = 1, startRow = 1)
  setColWidths(wb, "Analyse_Risque", cols = 2:4, widths = 40)
  
  colonnes <- colnames(entreprise_data)
  valeurs <- as.character(entreprise_data)
  
  headerStyle <- createStyle(fontSize = 14, fontColour = "green", halign = "center", fgFill = "#4F81BD", border = "TopBottom", borderColour = "red")
  bodyStyle <- createStyle(border = "TopBottom", borderColour = "orange")
  headerStyle2 <- createStyle(fontSize = 12, fontColour = "black", halign = "center", fgFill = "orange", border = "TopBottom", borderColour = "red")
  bodyStyle2 <- createStyle(border = "TopBottom", borderColour = "green")
  
  z <- "Confirmation  (1= total,2=partielle,3=pas de confirmation)"
  t <- SUIVI
  
  for (i in seq_along(colonnes)) {
    x <- colonnes[i]
    y <- valeurs[i]
    writeData(wb, "Analyse_Risque", x, startCol = 2, startRow = i + 1)
    writeData(wb, "Analyse_Risque", y, startCol = 3, startRow = i + 1)
  }
  
  setColWidths(wb, "Analyse_Risque", cols = 5, widths = 60)
  addStyle(wb, "Analyse_Risque", headerStyle, rows = 1, cols = 5, gridExpand = TRUE)
  writeData(wb, "Analyse_Risque", z, startCol = 5, startRow = 1)
  
  dataValidation(wb, "Analyse_Risque", col = 5, rows = 2, type = "whole", operator = "between", value = c(1, 3))
  
  addStyle(wb, "Analyse_Risque", headerStyle, rows = 1, cols = 7:20, gridExpand = TRUE)
  setColWidths(wb, "Analyse_Risque", cols = 7:20, widths = 40)
  writeData(wb, "Analyse_Risque", t, startCol = 7, startRow = 1)
  
  ## Fiche Contribuable (suite)
  for (i in seq_along(colonnes)) {
    x <- colonnes[i]
    y <- valeurs[i]
    writeData(wb, "Fiche_Contribuable", x, startCol = 2, startRow = i + 1)
    writeData(wb, "Fiche_Contribuable", y, startCol = 3, startRow = i + 1)
  }
  
  protectWorksheet(wb, "Fiche_Contribuable", protect = TRUE, password = "123Dcf")
  
  ## Fiche Pistes Investigation
  addWorksheet(wb, "FICHE_PISTES_INVESTIGATION")
  setColWidths(wb, "FICHE_PISTES_INVESTIGATION", cols = 1, widths = 0)
  setColWidths(wb, "FICHE_PISTES_INVESTIGATION", cols = 2, widths = 21)
  setColWidths(wb, "FICHE_PISTES_INVESTIGATION", cols = 3, widths = 40)
  setColWidths(wb, "FICHE_PISTES_INVESTIGATION", cols = 4:6, widths = 60)
  addStyle(wb, "FICHE_PISTES_INVESTIGATION", headerStyle, rows = 1, cols = 1:6, gridExpand = TRUE)
  addStyle(wb, "FICHE_PISTES_INVESTIGATION", bodyStyle, rows = 2:36, cols = 1:6, gridExpand = TRUE)
  FICHE_PISTES_INVESTIGATION[] <- lapply(FICHE_PISTES_INVESTIGATION, function(col) {
    if (is.character(col)) stringi::stri_enc_toutf8(col) else col
  })
  writeData(wb, "FICHE_PISTES_INVESTIGATION", FICHE_PISTES_INVESTIGATION)
  
  ## META DONNEES
  addWorksheet(wb, "META_DONNEES")
  setColWidths(wb, "META_DONNEES", cols = 1, widths = 10)
  setColWidths(wb, "META_DONNEES", cols = 2, widths = 21)
  setColWidths(wb, "META_DONNEES", cols = 3, widths = 40)
  setColWidths(wb, "META_DONNEES", cols = 4:16, widths = 60)
  addStyle(wb, "META_DONNEES", headerStyle2, rows = 1, cols = 1:6, gridExpand = TRUE)
  addStyle(wb, "META_DONNEES", bodyStyle2, rows = 2:61, cols = 1:6, gridExpand = TRUE)
  META_DONNEES[] <- lapply(META_DONNEES, function(col) {
    if (is.character(col)) stringi::stri_enc_toutf8(col) else col
  })
  writeData(wb, "META_DONNEES", META_DONNEES)
  
  ## Nomenclature SH
  addWorksheet(wb, "NomenclatureSH")
  setColWidths(wb, "NomenclatureSH", cols = 1, widths = 10)
  setColWidths(wb, "NomenclatureSH", cols = 2:3, widths = 40)
  setColWidths(wb, "NomenclatureSH", cols = 4, widths = 15)
  setColWidths(wb, "NomenclatureSH", cols = 5:6, widths = 60)
  addStyle(wb, "NomenclatureSH", headerStyle2, rows = 1, cols = 1:6, gridExpand = TRUE)
  addStyle(wb, "NomenclatureSH", bodyStyle2, rows = 2:61, cols = 1:6, gridExpand = TRUE)
  NomenclatureSH[] <- lapply(NomenclatureSH, function(col) {
    if (is.character(col)) stringi::stri_enc_toutf8(col) else col
  })
  writeData(wb, "NomenclatureSH", NomenclatureSH)
  
  ## Données Douane et TVA
  AN <- as.integer(entreprise_data$ANNEE_FISCAL)
  IFU_select <- entreprise_data$NUM_IFU
  Donnees_Douane <- subset(DGD, DGD$AN == AN & DGD$IFU == IFU_select)
  addWorksheet(wb, "Donnees_Douane")
  writeData(wb, "Donnees_Douane", Donnees_Douane)
  
  detail_tva1 <- subset(DCF_TVA_DEDUCTION, DCF_TVA_DEDUCTION$NUM_IFU_FOURN == IFU_select)
  detail_tva2 <- subset(DCF_TVA_DEDUCTION, DCF_TVA_DEDUCTION$NUM_IFU_CLIENT == IFU_select)
  DETAIL_TVA <- rbind(detail_tva1, detail_tva2)
  addWorksheet(wb, "DETAIL_TVA")
  writeData(wb, "DETAIL_TVA", DETAIL_TVA)
  
  ## Dossier et export
  dossier <- stringi::stri_enc_toutf8(as.character(entreprise_data$BRIGADES[1]))
  dossier <- gsub("[^A-Za-z0-9_]", "_", dossier)
  
  setwd("Z:/PV_Q1_2024/PROGRAMME_2025/")
  if (!dir.exists(dossier)) {
    dir.create(dossier)
  }
  
  chemins <- paste0("Z:/PV_Q1_2024/PROGRAMME_2025/", dossier, "/", contribuable, ".xlsx")
  chemins <- stringi::stri_enc_toutf8(chemins)
  
  saveWorkbook(wb, chemins, overwrite = TRUE)
}








