# Charger la bibliothèque
library(openxlsx)
library(readr)
library(stringr)
library(readxl)
library(igraph)
library(ggplot2)
setwd("Z:/PV_Q1_2024")

# éléments du répertoire courant :
list.files()
QUANTUM_prog<- read_excel("PROG_2025_Q2_HBS.xlsx")

QUANTUM_RSI<- read_excel("RSI_Q2_2025.xlsx",sheet = "Global")
QUANTUM_RNI<- read_excel("RNI_Q2_2025.xlsx",sheet = "FINAL")
QUANTUM_CME<- read_excel("CME_Q2_2025.xlsx",sheet = "Global")
QUANTUM_ND<- read_excel("ND_Q2_2025.xlsx",sheet = "Final")
#QUANTUM_ND<- read_excel("CSB_Q2_2025.xlsx")
QUANTUM<-rbind(QUANTUM_RSI,QUANTUM_RNI,QUANTUM_CME,QUANTUM_ND)

QUANTUM=QUANTUM[,c("NUM_IFU","NOM_MINEFID","ETAT","CODE_REG_FISC","ANNEE_FISCAL",
                   "DATE_DERNIERE_VG","DATE_DERNIERE_VP","DATE_DERNIERE_AVIS", "RISQUE_IND_15_A",
                   "RISQUE_IND_15_B","RISQUE_IND_1","GAP_IND_1","SCORE_IND_1",
                   "RISQUE_IND_2","GAP_IND_2", "SCORE_IND_2","RISQUE_IND_3", 
                   "RISQUE_IND_4","GAP_IND_4","SCORE_IND_4","RISQUE_IND_5" ,
                   "SCORE_IND_6","RISQUE_IND_7_A","RISQUE_IND_7_B","RISQUE_IND_8" ,     
                   "GAP_IND_8","SCORE_IND_8","RISQUE_IND_9","RISQUE_IND_10",     
                   "GAP_IND_10","SCORE_IND_10","RISQUE_IND_12","GAP_IND_12" ,       
                   "SCORE_IND_12","RISQUE_IND_13","GAP_IND_13","SCORE_IND_13" ,     
                   "RISQUE_IND_14","RISQUE_IND_16","RISQUE_IND_20","GAP_IND_20",        
                   "SCORE_IND_20","RISQUE_IND_21","RISQUE_IND_23","RISQUE_IND_24",     
                   "GAP_IND_24","SCORE_IND_24","RISQUE_IND_25","GAP_IND_25"  ,      
                   "SCORE_IND_25","RISQUE_IND_26","GAP_IND_26","SCORE_IND_26",      
                   "RISQUE_IND_27","RISQUE_IND_29","RISQUE_IND_30","RISQUE_IND_32" ,    
                   "RISQUE_IND_33","RISQUE_IND_34","RATIO_IND_37","RISQUE_IND_38",     
                   "RISQUE_IND_39")]



QUANTUM_final=left_join(QUANTUM_prog,QUANTUM,by="NUM_IFU")

QUANTUM_final$GAP_IND_1=round(as.numeric(QUANTUM_final$GAP_IND_1),0)
QUANTUM_final$GAP_IND_2=round(as.numeric(QUANTUM_final$GAP_IND_2),0)
#QUANTUM_final$GAP_IND_3=round(as.numeric(QUANTUM_final$GAP_IND_3),0)
QUANTUM_final$GAP_IND_4=round(as.numeric(QUANTUM_final$GAP_IND_4),0)
#QUANTUM_final$GAP_IND_5=round(as.numeric(QUANTUM_final$GAP_IND_5),0)
#QUANTUM_final$GAP_IND_6=round(as.numeric(QUANTUM_final$GAP_IND_6),0)
#QUANTUM_final$GAP_IND_7=round(as.numeric(QUANTUM_final$GAP_IND_7),0)
QUANTUM_final$GAP_IND_8=round(as.numeric(QUANTUM_final$GAP_IND_8),0)
#QUANTUM_final$GAP_IND_9=round(as.numeric(QUANTUM_final$GAP_IND_9),0)
QUANTUM_final$GAP_IND_10=round(as.numeric(QUANTUM_final$GAP_IND_10),0)
#QUANTUM_final$GAP_IND_11=round(as.numeric(QUANTUM_final$GAP_IND_11),0)
QUANTUM_final$GAP_IND_12=round(as.numeric(QUANTUM_final$GAP_IND_12),0)
QUANTUM_final$GAP_IND_13=round(as.numeric(QUANTUM_final$GAP_IND_13),0)
#QUANTUM_final$GAP_IND_14=round(as.numeric(QUANTUM_final$GAP_IND_14),0)
#QUANTUM_final$GAP_IND_15=round(as.numeric(QUANTUM_final$GAP_IND_15),0)
#QUANTUM_final$GAP_IND_16=round(as.numeric(QUANTUM_final$GAP_IND_16),0)
#QUANTUM_final$GAP_IND_17=round(as.numeric(QUANTUM_final$GAP_IND_17),0)
#QUANTUM_final$GAP_IND_18=round(as.numeric(QUANTUM_final$GAP_IND_18),0)
#QUANTUM_final$GAP_IND_19=round(as.numeric(QUANTUM_final$GAP_IND_19),0)
QUANTUM_final$GAP_IND_20=round(as.numeric(QUANTUM_final$GAP_IND_20),0)
#QUANTUM_final$GAP_IND_21=round(as.numeric(QUANTUM_final$GAP_IND_21),0)
#QUANTUM_final$GAP_IND_22=round(as.numeric(QUANTUM_final$GAP_IND_22),0)
#QUANTUM_final$GAP_IND_23=round(as.numeric(QUANTUM_final$GAP_IND_23),0)
QUANTUM_final$GAP_IND_24=round(as.numeric(QUANTUM_final$GAP_IND_24),0)
QUANTUM_final$GAP_IND_25=round(as.numeric(QUANTUM_final$GAP_IND_25),0)
QUANTUM_final$GAP_IND_26=round(as.numeric(QUANTUM_final$GAP_IND_26),0)
#QUANTUM_final$GAP_IND_27=round(as.numeric(QUANTUM_final$GAP_IND_27),0)
#QUANTUM_final$GAP_IND_28=round(as.numeric(QUANTUM_final$GAP_IND_28),0)
#QUANTUM_final$GAP_IND_29=round(as.numeric(QUANTUM_final$GAP_IND_29),0)
#QUANTUM_final$GAP_IND_30=round(as.numeric(QUANTUM_final$GAP_IND_30),0)
#QUANTUM_final$GAP_IND_31=round(as.numeric(QUANTUM_final$GAP_IND_31),0)
#QUANTUM_final$GAP_IND_32=round(as.numeric(QUANTUM_final$GAP_IND_32),0)
#QUANTUM_final$GAP_IND_33=round(as.numeric(QUANTUM_final$GAP_IND_33),0)
#QUANTUM_final$GAP_IND_34=round(as.numeric(QUANTUM_final$GAP_IND_34),0)
#QUANTUM_final$GAP_IND_35=round(as.numeric(QUANTUM_final$GAP_IND_35),0)
#QUANTUM_final$GAP_IND_36=round(as.numeric(QUANTUM_final$GAP_IND_36),0)
#QUANTUM_final$GAP_IND_37=round(as.numeric(QUANTUM_final$GAP_IND_37),0)
#QUANTUM_final$GAP_IND_38=round(as.numeric(QUANTUM_final$GAP_IND_38),0)
#QUANTUM_final$GAP_IND_39=round(as.numeric(QUANTUM_final$GAP_IND_39),0)


FICHE_PISTES_INVESTIGATION <- read_excel("FICHE_PISTES_INVESTIGATION.xlsx")
SUIVI <- read_excel("SUIVI.xlsx")
META_DONNEES <- read_excel("META_DONNEES.xlsx")
names(SUIVI)=c("NOTIF_INITIAL_REF","NOTIF_INITIAL_IMPOT","NOTIF_INITIAL_DS","NOTIF_INITIAL_PEN","NOTIF_FINAL_REF","NOTIF_FINAL_DS","NOTIF_FINAL_PEN","NOTIF_FINAL_PEN","TITRE_EMIS_REF","TITRE_EMIS_DS"  ,"TITRE_EMIS_PEN", "TITRE_EMIS_PEN","OBSERVATION")


con<- DBI::dbConnect(drv = RPostgres::Postgres(),# a remplacer par ods
                     host = "10.3.1.129",
                     port = 5432,
                     dbname = "postgres",
                     user = "postgres",
                     password = "cpf2022")

#=dbReadTable(con,'SID_DCF_DGD') 
channel <- dbConnect(odbc::odbc(),
                     Driver = "Oracle in XE",               # nom du driver exact
                     Dbq    = "10.3.1.32:1521/SIDDGI",  # chaîne Easy Connect (host:port/service_name)
                     UID    = "ODSDI1",
                     PWD    = "odsdi1")

DGD=dbReadTable(channel,'SID_DGD_CPF') 

#write.csv(DGD,"DGD.csv")
DGD$ANNEE=substr(DGD$DATE_LIQUIDATION,1,4)


#IFU_conex <- dbConnect("IFU_con", uid="toussaintifu", pwd="toussaint2025")
IFU_conex <- dbConnect(odbc::odbc(),
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
IFU<- dbGetQuery(IFU_conex, ReqIFU)

names(IFU)=c("SOUS_UR","NUM_IFU","LATITUDE","LONGITUDE","ACTIVITES_IFU")

IFU_2=IFU[,c("SOUS_UR","NUM_IFU","LATITUDE","LONGITUDE")]


QUANTUMS=inner_join(IFU_2,QUANTUM_final,by="NUM_IFU")


#sheet_namesP <- excel_sheets("Z:/PV_Q1_2024/Q1_2024/Q1.xlsx")
#Q1 <- read_excel("Z:/PV_Q1_2024/Q1_2024/Q1.xlsx", range = "B8:C100")
#nb_sheet_namesP <- length(sheet_namesP)
#PROG_l <-NULL
#PROG_l_tmp <-NULL
#for(i in 1:nb_sheet_namesP){ nameIFU <- sheet_namesP[i] 
#PROG_l_tmp <- read_excel("Z:/PV_Q1_2024/Q1_2024/Q1.xlsx",sheet =nameIFU , range = "B8:C100")
#PROG_l<- rbind(PROG_l_tmp,PROG_l)
#}

#PROG_l=unique(PROG_l)
#dim(PROG_l)

#l_Q1=unique(PROG_l$NUM_IFU)

#QUANTUMS_janv=subset(QUANTUMS,QUANTUMS$NUM_IFU %in% l_Q1)


#donnees<-QUANTUMS_janv

donnees<-QUANTUMS

donnees$SOUS_UR=str_replace(donnees$SOUS_UR, pattern = "/", replacement = "_")
donnees$SOUS_UR=str_replace(donnees$SOUS_UR, pattern = "-", replacement = "_")
donnees$SOUS_UR=str_replace(donnees$SOUS_UR, pattern = " ", replacement = "_")


donnees$UR=str_replace(donnees$UR, pattern = "/", replacement = "_")
donnees$UR=str_replace(donnees$UR, pattern = "-", replacement = "_")

donnees$BRIGADES=str_replace(donnees$BRIGADES, pattern = "/", replacement = "_")
donnees$BRIGADES=str_replace(donnees$BRIGADES, pattern = "-", replacement = "_")


donnees$DATE_DERNIERE_AVIS=year(donnees$DATE_DERNIERE_AVIS)
donnees$DATE_DERNIERE_AVIS=as.numeric(donnees$DATE_DERNIERE_AVIS)
donnees$DATE_DERNIERE_VG=year(donnees$DATE_DERNIERE_VG)
donnees$DATE_DERNIERE_VG=as.numeric(donnees$DATE_DERNIERE_VG)
donnees$DATE_DERNIERE_VP=year(donnees$DATE_DERNIERE_VP)
donnees$DATE_DERNIERE_VP=as.numeric(donnees$DATE_DERNIERE_VP)

#donnees=head(donnees,2)

# Créer une liste unique des noms de contribuable
#contribuables <- unique(donnees$NUM_IFU)
#contribuables=paste0(donnees$SOUS_UR,"_",donnees$BRIGADES,"_IFU",donnees$NUM_IFU,"_AN",donnees$ANNEE_FISCAL)
contribuables=paste0(donnees$NUM_IFU,'_',donnees$SOUS_UR,"_",donnees$BRIGADES)

contribuables=str_replace(contribuables, pattern = "/", replacement = "_")
contribuables=str_replace(contribuables, pattern = "-", replacement = "_")
donnees$contribuables=contribuables
donnees$contribuables <- stri_enc_toutf8(donnees$contribuables)

# donnees=head(donnees,3)

contribuable=NULL
# Parcourir la liste des contribuables et exporter les données correspondantes dans des classeurs Excel
for (contribuable in donnees$contribuables) {
  # Filtrer les données pour le contribuable spécifique
  entreprise_data <- subset(donnees, donnees$contribuables == contribuable)
  
  IFU_CONTR= substr(contribuable,1,9)
  
  UR=entreprise_data$UR[1]
  BV=entreprise_data$BRIGADES[1]
  SUR=entreprise_data$SOUS_UR[1]
  
  entreprise_data_2022=subset(entreprise_data, entreprise_data$ANNEE_FISCAL==2022)
  entreprise_data_2023=subset(entreprise_data, entreprise_data$ANNEE_FISCAL==2023)
  entreprise_data_2024=subset(entreprise_data, entreprise_data$ANNEE_FISCAL==2024)
  
  # Créer un nouveau classeur Excel
  wb <- createWorkbook()
  
  # Ajouter une feuille de calcul au classeur
  addWorksheet(wb, sheetName = "Fiche_Contribuable")
  writeData(wb, sheet = "Fiche_Contribuable", x = contribuable, startCol = 1, startRow = 1)
  writeData(wb, sheet = "Fiche_Contribuable", x = contribuable, startCol = 1, startRow = 1)
  setColWidths(wb, "Fiche_Contribuable", cols = 2:4, widths = 40) ## set column width for row names column
  
  
  addWorksheet(wb, sheetName = "Analys_Risque")
  setColWidths(wb, "Analys_Risque", cols = 2:4, widths = 40) ## set column width for row names column
  
  
  # Ajouter le nom du contribuable à la première ligne
  writeData(wb, sheet = "Analys_Risque", x = contribuable, startCol = 1, startRow = 1)
  writeData(wb, sheet = "Analys_Risque", x = contribuable, startCol = 1, startRow = 1)
  
  # Récupérer les colonnes et leurs valeurs (en excluant les deux premiers champs)
  #colonnes <- colnames(entreprise_data)[-c(1, 2,3)]  # Exclure les deux premières colonnes
  #valeurs <- as.character(entreprise_data[1, -c(1, 2,3)])  # Exclure les deux premières colonnes
  
  colonnes <- colnames(entreprise_data)[-c(1)]  # Exclure les deux premières colonnes
  valeurs <- as.character(entreprise_data_2022[1, -c(1)])  # Exclure les deux premières colonnes
  valeurs1 <- as.character(entreprise_data_2023[1, -c(1)])  # Exclure les deux premières colonnes
  valeurs2 <- as.character(entreprise_data_2024[1, -c(1)])  # Exclure les deux premières colonnes
  
  
  
  
  
  
  
  
  headerStyle <- createStyle(
    fontSize = 14, 
    fontColour = "green", 
    halign = "center",
    fgFill = "#4F81BD", 
    border = "TopBottom", 
    borderColour = "red"
  ) 
  
  bodyStyle <- createStyle(border = "TopBottom", borderColour = "orange")
  
  headerStyle2 <- createStyle(
    fontSize = 12, 
    fontColour = "black", 
    halign = "center",
    fgFill = "orange", 
    border = "TopBottom", 
    borderColour = "red"
  ) 
  
  bodyStyle2 <- createStyle(border = "TopBottom", borderColour = "green")
  
  
  
  # Ajouter les colonnes et valeurs dans chaque colonne Excel
  for (i in seq_along(colonnes)) {
    #writeData(wb, sheet = "Donnees", x = paste0(colonnes[i], ": ", valeurs[i]), startCol = i, startRow = 2)
    x= colonnes[i]
    y=valeurs[i]
    
    
    z="Confirmation  (1= total,2=partielle,3=pas de confirmation)"
    t=SUIVI
    writeData(wb, sheet = "Analys_Risque", x , startCol = 2, startRow = i+1)
    writeData(wb, sheet = "Analys_Risque", y , startCol = 3, startRow = i+1)
    
  }
  setColWidths(wb, "Analys_Risque", cols = 5, widths = 60) ## set column width for row names column
  addStyle(wb, "Analys_Risque", headerStyle, rows = 1, cols = 5, gridExpand = TRUE)
  
  
  writeData(wb, sheet = "Analys_Risque", z , startCol = 5, startRow =1 )
  
  dataValidation(wb, "Analys_Risque",
                 col = 5, rows = 2, type = "whole",
                 operator = "between", value = c(1, 3)
  )
  
  addStyle(wb, "Analys_Risque", headerStyle, rows =1 , cols = 7:20, gridExpand = TRUE)
  setColWidths(wb, "Analys_Risque", cols = 7:20, widths = 40) ## set column width for row names column
  
  writeData(wb, sheet = "Analys_Risque", t , startCol = 7, startRow =1 )
  
  
  # Ajouter les colonnes et valeurs dans chaque colonne Excel
  for (i in seq_along(colonnes)) {
    #writeData(wb, sheet = "Donnees", x = paste0(colonnes[i], ": ", valeurs[i]), startCol = i, startRow = 2)
    x= colonnes[i]
    y=valeurs[i]
    y1=valeurs1[i]
    y2=valeurs2[i]
    writeData(wb, sheet = "Fiche_Contribuable", x , startCol = 2, startRow = i+1)
    writeData(wb, sheet = "Fiche_Contribuable", y , startCol = 3, startRow = i+1)
    writeData(wb, sheet = "Fiche_Contribuable", y1 , startCol = 4, startRow = i+1)
    writeData(wb, sheet = "Fiche_Contribuable", y2 , startCol = 5, startRow = i+1)
    
  }
  
  protectWorksheet(
    wb,
    "Fiche_Contribuable",
    protect = TRUE,
    password = "123Dcf")
  
  # Enregistrer le classeur Excel avec le nom du contribuable
  
  
  addWorksheet(wb, sheetName = "FICHE_PISTES_INVESTIGATION")
  
  
  # Mise en forme des largeur des cellules
  
  setColWidths(wb, "FICHE_PISTES_INVESTIGATION", cols = 1, widths = 0) ## set column width for row names colum
  
  setColWidths(wb, "FICHE_PISTES_INVESTIGATION", cols = 2, widths = 21) ## set column width for row names column
  setColWidths(wb, "FICHE_PISTES_INVESTIGATION", cols = 3, widths = 40) ## set column width for row names column
  
  setColWidths(wb, "FICHE_PISTES_INVESTIGATION", cols = 4:6, widths = 60) ## set column width for row names column
  
  # Mise en forme entete
  
  addStyle(wb, "FICHE_PISTES_INVESTIGATION", headerStyle, rows = 1, cols = 1:6, gridExpand = TRUE)
  
  # Mise en forme corps
  
  addStyle(wb, "FICHE_PISTES_INVESTIGATION", bodyStyle, rows = 2:36, cols = 1:6, gridExpand = TRUE)
  
  writeData(wb, sheet = "FICHE_PISTES_INVESTIGATION", FICHE_PISTES_INVESTIGATION)
  
  
  
  addWorksheet(wb, sheetName = "META_DONNEES")
  
  
  # Mise en forme des largeur des cellules
  
  setColWidths(wb, "META_DONNEES", cols = 1, widths = 10) ## set column width for row names colum
  
  setColWidths(wb, "META_DONNEES", cols = 2, widths = 21) ## set column width for row names column
  setColWidths(wb, "META_DONNEES", cols = 3, widths = 40) ## set column width for row names column
  
  setColWidths(wb, "META_DONNEES", cols = 4:16, widths = 60) ## set column width for row names column
  
  # Mise en forme entete
  
  addStyle(wb, "META_DONNEES", headerStyle2, rows = 1, cols = 1:6, gridExpand = TRUE)
  
  # Mise en forme corps
  
  addStyle(wb, "META_DONNEES", bodyStyle2, rows = 2:61, cols = 1:6, gridExpand = TRUE)
  
  writeData(wb, sheet = "META_DONNEES", META_DONNEES)
  
  
  
  
  D_UR=paste0("Z:/PV_Q1_2024/PROGRAMME2025_Q2","/",UR)
  ifelse(dir.exists(D_UR)==TRUE,print("dossier crée"),dir.create(D_UR))
  UR=unique(UR)
  
  D_SUR=paste0("Z:/PV_Q1_2024/PROGRAMME2025_Q2","/",UR,"/",SUR)
  
  ifelse(dir.exists(D_SUR)==TRUE,print("dossier crée"),dir.create(D_SUR))
  
  
  D_BV=paste0("Z:/PV_Q1_2024/PROGRAMME2025_Q2","/",UR,"/",SUR,"/",BV)
  
  ifelse(dir.exists(D_BV)==TRUE,print("dossier crée"),dir.create(D_BV))
  
  setwd(D_BV)
  
  
  NomenclatureSH <- read_excel("Z:/PV_Q1_2024/NomenclatureSH.xlsx")
  addWorksheet(wb, sheetName = "NomenclatureSH")
  writeData(wb, sheet = "NomenclatureSH", x = NomenclatureSH, startCol = 1, startRow = 1)
  
  
  addWorksheet(wb, sheetName = "Donnees_douanieres")
  Donnees_douanieres=subset(DGD,DGD$IFU==IFU_CONTR)
  Donnees_douanieres=Donnees_douanieres[,c('CODE_BUREAU',	'LIB_BUREAU',	'NUM_LIQUIDATION',	'DATE_LIQUIDATION',	'NUM_ARTICLE',	'LIBELLE',	'PAYS_ORIGINE',	'PAYS_DESTINATION1',	'PAYS_DESTINATION_FINALE',	'FLUX',	'REGIME',	'TYPE_DECLARATION',	'IFU',	'ENTREPRISE',	'LIBELLE_BUREAU_FRONTIERE',	'PDS_NET',	'PDS_BRT',	'QUANTITE',	'CAF',	'FOB',	'DATE_ENREGISTREMENT',	'COMPTE',	'LIBELLE_MODE_TRANSPORT',	'NUM_DECLARATION',	'DECLARANT',	'LIB_DECLARANT',	'DD',	'CSE',	'RS',	'TSB',	'TSC',	'TST',	'TIC',	'TVA',	'IFU_DECLARANT',	'ACOMPTE',	'ETAT',	'ANNULEE',	'TEP',	'TPC',	'TVT')]
  Donnees_douanieres[] <- lapply(Donnees_douanieres, function(x) {
    if (is.character(x)) stri_enc_toutf8(x, validate = TRUE) else x
  })
  writeData(wb, sheet = "Donnees_douanieres", x = Donnees_douanieres, startCol = 1, startRow = 1)
  
  
  
  addWorksheet(wb, sheetName = "Client_Detail_TVA")
  Client_Detail_TVA=subset(DCF_TVA_DEDUCTION,DCF_TVA_DEDUCTION$NUM_IFU_FOURN==IFU_CONTR)
  Client_Detail_TVA[] <- lapply(Client_Detail_TVA, function(x) {
    if (is.character(x)) stri_enc_toutf8(x, validate = TRUE) else x
  })
  writeData(wb, sheet = "Client_Detail_TVA", x = Client_Detail_TVA, startCol = 1, startRow = 1)
  
  
  addWorksheet(wb, sheetName = "Fournisseur_Detail_TVA")
  Fournisseur_Detail_TVA=subset(DCF_TVA_DEDUCTION,DCF_TVA_DEDUCTION$NUM_IFU_CLIENT==IFU_CONTR)
  Fournisseur_Detail_TVA[] <- lapply(Fournisseur_Detail_TVA, function(x) {
    if (is.character(x)) stri_enc_toutf8(x, validate = TRUE) else x
  })
  writeData(wb, sheet = "Fournisseur_Detail_TVA", x = Fournisseur_Detail_TVA, startCol = 1, startRow = 1)
  
  #reseau_contr=rbind(Fournisseur_Detail_TVA,Client_Detail_TVA)
  
  if(nrow(Fournisseur_Detail_TVA)==0) {
    print("pas de relation TVA declarée")
  }else
  {
    
    TVA_RESAU_res1=Fournisseur_Detail_TVA[,c("NUM_IFU_CLIENT",'ID_IMPOT',"NUM_IFU_FOURN" )]
    names(TVA_RESAU_res1)=c('NUM_IFU','ID_IMPOT','IFU')
    TVA_res1=TVA_RESAU_res1
    TVA_res1=unique(TVA_res1)
    TVA_res1$IFU[is.na(TVA_res1$IFU)|TVA_res1$IFU ==""]="IFU NON RENSEIGNE"
    
    TVA_res1=aggregate(ID_IMPOT~NUM_IFU+IFU,TVA_res1,length)
    
    TVA_res1$IFU=ifelse(TVA_res1$IFU==0,"IFU NON RENSEIGNE",TVA_res1$IFU)
    TVA_res1=TVA_res1[order(TVA_res1$ID_IMPOT,decreasing=T),]
    TVA_res1=head(TVA_res1,25)
    
    n_TVA_res1=graph.data.frame(TVA_res1,directed = T)
    
    nom_reseau1=paste0(D_BV,"/",contribuable,"_F.png")
    png(file = nom_reseau1, width = 800, height = 700)
    plot(n_TVA_res1)
    dev.off()
    
    addWorksheet(wb, sheetName = "Reseau_Client")
    insertImage(wb, sheet = "Reseau_Client", 
                nom_reseau1,
                width = 2400, height = 2400,units = "px"
                ,startRow = 3, startCol = "D"
    )
    
  } 
  
  if(nrow(Client_Detail_TVA)==0) {
    print("pas de relation TVA declarée")
  }else
  {
    
    TVA_RESAU_res2=Client_Detail_TVA[,c("NUM_IFU_CLIENT",'ID_IMPOT',"NUM_IFU_FOURN" )]
    names(TVA_RESAU_res2)=c('NUM_IFU','ID_IMPOT','IFU')
    TVA_res2=TVA_RESAU_res2
    TVA_res2=unique(TVA_res2)
    TVA_res2$IFU[is.na(TVA_res2$IFU)|TVA_res2$IFU ==""]="IFU NON RENSEIGNE"
    TVA_res2=aggregate(ID_IMPOT~NUM_IFU+IFU,TVA_res2,length)
    
    TVA_res2$IFU=ifelse(TVA_res2$IFU==0,"IFU NON RENSEIGNE",TVA_res2$IFU)
    
    TVA_res2=TVA_res2[order(TVA_res2$ID_IMPOT,decreasing=T),]
    TVA_res2=head(TVA_res2,25)
    
    n_TVA_res2=graph.data.frame(TVA_res2,directed = T)
    
    nom_reseau2=paste0(D_BV,"/",contribuable,"_C.png")
    png(file = nom_reseau2, width = 800, height = 700)
    plot(n_TVA_res2)
    dev.off()
    
    addWorksheet(wb, sheetName = "Reseau_Fourn")
    insertImage(wb, sheet = "Reseau_Fourn", 
                nom_reseau2,
                width = 2400, height = 2400,units = "px"
                ,startRow = 3, startCol = "D"
    )
    
  }  
  chemins=paste0(D_BV,"/",contribuable, ".xlsx")
  print(chemins)
  # print(head(entreprise_data))
  saveWorkbook(wb, chemins, overwrite = TRUE)
  
  #saveWorkbook(wb, paste0(D_BV,"/",contribuable, ".xlsx"), overwrite = TRUE)
}


