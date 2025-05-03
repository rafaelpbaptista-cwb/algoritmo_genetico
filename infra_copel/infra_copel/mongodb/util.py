# -*- coding: utf-8 -*-
"""Funções úteis."""

import pandas as pd


def df_to_docs(df_in: pd.DataFrame, del_null_keys: bool = True) -> list[dict]:
    """
    Transforma um dataframe em 'records' para uso no MongoDB.

    Parameters
    ----------
    df_in : DataFrame
        Dataframe a ser transformado.

    Returns
    -------
    list[dict]

    """
    df_in = df_in.copy()
    for column, dtype in df_in.dtypes.items():
        # Muda os Period para Timestamp
        if isinstance(dtype, pd.core.dtypes.dtypes.PeriodDtype):
            df_in[column] = df_in[column].dt.to_timestamp()

    records = df_in.to_dict("records")
    # Apaga as chaves dos dicionários com valor nulo
    if del_null_keys:
        return [
            {k: record[k] for k, v in record.items() if not pd.isna(v)}
            for record in records
        ]

    return records


def extend_period_seasonal(
    dataframe: pd.DataFrame,
    periods: int | None,
) -> pd.DataFrame:
    """Extende os dados do período de forma sazonal.

    Parameters
    ----------
    dataframe : DataFrame
        Dados a serem extendidos.
    periods : int | None
        Quantidade de períodos a extender.

    Returns
    -------
    DataFrame

    """
    period_extended = pd.period_range(dataframe.index[-1] + 1, periods=periods)

    df_extended = dataframe.reindex(dataframe.index.union(period_extended))

    return df_extended.groupby(df_extended.index.month).ffill()
