library(openxlsx)
library(stringi)

# Étape 1 — Identifier les dataframes à exporter
noms_df <- ls(pattern = "^ELIGIBLE_IND[0-9]+$")
noms_df <- c(noms_df, "Global")  # Ajouter Global manuellement
dfs <- mget(noms_df)             # Créer la liste nommée

# Étape 2 — Initialiser le classeur Excel et une liste pour les erreurs
wb <- createWorkbook()
feuilles_ratees <- c()

# Étape 3 — Parcourir les dataframes et exporter proprement
for (name in names(dfs)) {
  tryCatch({
    message("🔄 Traitement de : ", name)
    
    # Nettoyage du nom d'onglet (max 31 caractères, UTF-8)
    sheet_name <- iconv(name, from = "", to = "UTF-8", sub = "")
    sheet_name <- substr(sheet_name, 1, 31)
    
    # Extraction du data.frame
    df <- dfs[[name]]
    
    # Nettoyage du contenu : suppression des caractères invalides
    df[] <- lapply(df, function(col) {
      if (is.character(col)) iconv(col, from = "", to = "UTF-8", sub = "") else col
    })
    
    # Ajouter la feuille et écrire les données
    addWorksheet(wb, sheet_name)
    writeData(wb, sheet_name, df)
    
  }, error = function(e) {
    message(sprintf("⛔ Problème avec '%s' : %s", name, e$message))
    feuilles_ratees <<- c(feuilles_ratees, name)
  })
}

# Étape 4 — Sauvegarde du classeur Excel
saveWorkbook(wb, "C:/Users/Administrateur/Desktop/Systeme_Decisionnel/eligibles_csb.xlsx", overwrite = TRUE)
message("✅ Fichier 'export_final.xlsx' généré avec succès.")

# Étape 5 — Sauvegarde des erreurs éventuelles
if (length(feuilles_ratees) > 0) {
  writeLines(feuilles_ratees, "feuilles_non_exportees.txt")
  message("⚠️ Certaines feuilles n'ont pas été exportées. Voir 'feuilles_non_exportees.txt'")
} else {
  message("🎉 Toutes les feuilles ont été exportées avec succès !")
}

#saveWorkbook(wb, "test_elig1.xlsx", overwrite = TRUE)


# Sauvegarde
#saveWorkbook(wb, "C:/Users/Administrateur/Desktop/Systeme_Decisionnel/Mes_data.xlsx", overwrite = TRUE)
