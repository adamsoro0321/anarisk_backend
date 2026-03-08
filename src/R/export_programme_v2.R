#################Exportation des classeurs du T1 ###############################################################################################

#################Exportation des classeurs du T1 ###############################################################################################

# Exportation des feuilles 
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
Q2 <- read_excel("Z:/PV_Q1_2024/Q2.xlsx")

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
