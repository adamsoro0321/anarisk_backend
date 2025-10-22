import pandas as pd


def df_columns_process(
    df, columns_list, duplicate_check_columns=None, handle_duplicates="drop"
):
    """
    Sélectionne des colonnes spécifiques d'un DataFrame et gère les doublons.

    Args:
        df (pd.DataFrame): DataFrame d'entrée
        columns_list (list): Liste des noms de colonnes à sélectionner
        duplicate_check_columns (list, optional): Liste des colonnes sur lesquelles vérifier les doublons.
                                                 Par défaut None (pas de vérification)
        handle_duplicates (str): Comment gérer les doublons ('drop', 'keep_first', 'keep_last', 'mark')
                                'drop' : supprime tous les doublons sauf le premier
                                'keep_first' : garde le premier doublon (équivalent à 'drop')
                                'keep_last' : garde le dernier doublon
                                'mark' : ajoute une colonne 'IS_DUPLICATE' pour marquer les doublons

    Returns:
        pd.DataFrame: Nouveau DataFrame avec les colonnes sélectionnées et doublons gérés

    Example:
        # Vérifier les doublons sur NUM_IFU et ANNEE
        result = df_columns_process(df, ['NUM_IFU', 'ANNEE', 'CA'],
                                   duplicate_check_columns=['NUM_IFU', 'ANNEE'])
    """
    # Sélectionner les colonnes demandées
    available_columns = [col for col in columns_list if col in df.columns]
    missing_columns = [col for col in columns_list if col not in df.columns]

    if missing_columns:
        print(f"⚠️  Colonnes manquantes ignorées: {missing_columns}")

    if not available_columns:
        print("❌ Aucune colonne disponible trouvée!")
        return pd.DataFrame()

    result_df = df[available_columns].copy()

    # Vérification des doublons si spécifiée
    if duplicate_check_columns:
        # Vérifier que les colonnes de vérification existent
        check_cols_available = [
            col for col in duplicate_check_columns if col in result_df.columns
        ]
        check_cols_missing = [
            col for col in duplicate_check_columns if col not in result_df.columns
        ]

        if check_cols_missing:
            print(f"⚠️  Colonnes de vérification manquantes: {check_cols_missing}")

        if check_cols_available:
            # Identifier les doublons
            duplicates_mask = result_df.duplicated(
                subset=check_cols_available, keep=False
            )
            duplicate_count = duplicates_mask.sum()

            if duplicate_count > 0:
                print(
                    f"🔍 Doublons détectés: {duplicate_count} lignes avec des combinaisons dupliquées sur {check_cols_available}"
                )

                # Afficher quelques exemples de doublons
                duplicates_sample = (
                    result_df[duplicates_mask][check_cols_available]
                    .drop_duplicates()
                    .head(5)
                )
                print("📋 Exemples de combinaisons dupliquées:")
                for _, row in duplicates_sample.iterrows():
                    combination = " + ".join(
                        [f"{col}={row[col]}" for col in check_cols_available]
                    )
                    count = len(
                        result_df[
                            (
                                result_df[check_cols_available]
                                == row[check_cols_available]
                            ).all(axis=1)
                        ]
                    )
                    print(f"   • {combination} ({count} occurrences)")

                # Gérer les doublons selon la stratégie choisie
                if handle_duplicates == "drop" or handle_duplicates == "keep_first":
                    result_df = result_df.drop_duplicates(
                        subset=check_cols_available, keep="first"
                    )
                    print(f"✅ Doublons supprimés. Lignes restantes: {len(result_df)}")

                elif handle_duplicates == "keep_last":
                    result_df = result_df.drop_duplicates(
                        subset=check_cols_available, keep="last"
                    )
                    print(
                        f"✅ Doublons supprimés (gardé le dernier). Lignes restantes: {len(result_df)}"
                    )

                elif handle_duplicates == "mark":
                    result_df["IS_DUPLICATE"] = result_df.duplicated(
                        subset=check_cols_available, keep="first"
                    )
                    print("🏷️  Doublons marqués dans la colonne 'IS_DUPLICATE'")

            else:
                print(
                    f"✅ Aucun doublon détecté sur les colonnes {check_cols_available}"
                )

    print(
        f"📊 DataFrame final: {len(result_df)} lignes, {len(result_df.columns)} colonnes"
    )
    return result_df
