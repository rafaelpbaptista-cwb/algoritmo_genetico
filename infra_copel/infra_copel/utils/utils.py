# -*- coding: utf-8 -*-
"""
Created on Mon Aug 21 11:24:15 2023

@author: Copel
"""
from datetime import datetime
import pandas as pd

from infra_copel import MongoHistoricoOficial


def analisar_data_calendario_operativa(data: datetime) -> dict:
    """
    Análise da data informada de acordo com o calendário operativo.

    Parameters
    ----------
    data : str
        data a ser analisada

    Returns
    -------
    dict

    """
    dia = pd.Period(data, freq="D")
    sem_op = dia.asfreq("W-FRI")
    mes_op = pd.Period(sem_op.end_time, freq="M")
    semanas_op = pd.period_range(mes_op.start_time, mes_op.end_time, freq="W-FRI")
    # retira a última semana caso ela não seja do mês operativo atual
    if semanas_op[-1].end_time.month != mes_op.end_time.month:
        semanas_op = semanas_op[:-1]
    rev = semanas_op.get_loc(sem_op)
    n_sem_op = rev + 1
    dia_semana = dia.day_of_week
    ult_rev = True if n_sem_op == len(semanas_op) else False
    res = {
        "dia": dia,
        "sem_op": sem_op,
        "mes_op": mes_op,
        "semanas": semanas_op,
        "rev": rev,
        "semana": n_sem_op,
        "dia_semana": dia_semana,
        "ult_rev": ult_rev,
    }

    return res


def extend_period_seasonal(
    dataframe: pd.DataFrame,
    periods: int | None,
) -> pd.DataFrame:
    """
    Extende os dados do período de forma sazonal.

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


def ajustar_limites_pld(
    df_cmo_submercado: pd.DataFrame, ano: int = datetime.now().year
) -> None:
    """
    Ajusta o dataframe contendo os preços de CMO com os limites de PLD

    Parameters
    ----------
    df_cmo_submercado : DataFrame
        Dados de CMO, agrupados por submercado
    """
    df_limites_pld_tarifas = MongoHistoricoOficial().df_limites_pld_tarifas

    pld_min = df_limites_pld_tarifas.loc[str(ano)]["PLD_MIN"]
    pld_max = df_limites_pld_tarifas.loc[str(ano)]["PLD_MAX_EST"]

    df_cmo_submercado.clip(lower=pld_min, upper=pld_max, inplace=True)
