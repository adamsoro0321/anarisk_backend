import pandas as pd


def sintaxDF(engine, batch_size=400):
    """
    Cette fonction se connecte à oracle sur Sintax et effectue une
    requête SQL à l'aide de pandas.
    Retourne un DataFrame.
    """
    sql = """
    SELECT DISTINCT 
        str.NUMERO_QUITTANCE, 
        TO_CHAR(TO_DATE(str.DATE_RGLT), 'YYYY-MM-DD') AS DATE_RGLT, 
        str.ID_CONTR, 
        str.MONTANT_RGLT, 
        str.MONTANT_PERCU
    FROM 
        S_T_REGLEMENT str
    WHERE str.STATUT_TAMPON = 1
    AND str.etat_rglt = 'VALIDE'
    AND str.TYPE_RGLT <> 'MANUEL'
    AND str.NUMERO_QUITTANCE IN (
        SELECT NUMERO_QUITTANCE 
        FROM S_T_REGLEMENT_IMPOT STRI2
        INNER JOIN E_T_IMPOT ETI2 ON STRI2.ID_IMPOT = ETI2.ID_IMPOT
    )
    FETCH FIRST :batch_size ROWS ONLY
    """

    try:
        return pd.read_sql(sql, engine, params={"batch_size": batch_size})
    except Exception as e:
        print(f"Error: {e}")
        return pd.DataFrame()  # Return an empty DataFrame in case of error
