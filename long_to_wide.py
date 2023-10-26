def long_to_wide(df_long_input, pivot_varlist):
    """

    :param df_long_input:
    :param pivot_varlist: Liste de variables sur lesquelles la table est pivotée
    :return: DataFrame en format wide
    """
    df_wide = (
        df_long_input
        .set_index(pivot_varlist)
        .unstack()
        .reset_index()
    )
    # écraser le multiIndex
    df_wide.columns = ["".join(x) for x in df_wide.columns.to_flat_index()]
    return df_wide
