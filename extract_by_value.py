def extract_by_value(input_df, colname, varvalue):
    """

    :param input_df: DataFrame données statcan
    :param colname: Nom de la colonne à filtrer
    :param varvalue: valeurs du filtre
    :return:
    """
    df = input_df[input_df[colname].isin(varvalue)]
    return df
