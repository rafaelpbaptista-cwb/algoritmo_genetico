# -*- coding: utf-8 -*-
"""Formatações de dataframe específicas para o powerbi."""

import pandas as pd
import numpy as np


def add_agregados(df_in: pd.DataFrame) -> pd.DataFrame:
    """
    Adiciona colunas e linha adicional no dataframe.

    Parameters
    ----------
    df_in : DataFrame
        Dataframe de entrada.
        As colunas devem estar no formato de PeriodIndex.

    Returns
    -------
    DataFrame
        Dataframe modificado.

    """
    # Para não mudar o original informado
    df = df_in.copy()

    # Muda o nome das colunas para texto
    df.columns = df.columns.strftime('%Y-%m')

    # Adiciona colunas de agregados
    df['3MES'] = df.iloc[:, :3].mean(axis=1)
    df['12MES'] = df.iloc[:, :12].mean(axis=1)
    df['TOTAL'] = df.mean(axis=1)

    # Adiciona linha de média total
    df.loc['MEDIA TOTAL'] = df.mean()

    return df


def simplificar_index(df_in: pd.DataFrame,
                      str_rolling: bool = True,
                      data_inicial: bool = False,
                      perc_friccao: bool = True,
                      nome_usinas: bool = True,
                      ) -> pd.DataFrame:
    """
    Simplifica o nome da serie que está no index.

    Parameters
    ----------
    df_in : DataFrame
        Dataframe cujo index será simplificado.
    str_rolling : bool, optional
        Apagar a string Rolling? The default is True.
    data_inicial : bool, optional
        Apagar data inicial? The default is False.
    perc_friccao : bool, optional
        Apagar percentual de friccao? The default is True.
    nome_usinas : bool, optional
        Apagar nome das usinas? The default is True.

    Returns
    -------
    DataFrame

    """
    # Para não mudar o original informado
    df = df_in.copy()
    # Apaga a string Rolling
    if str_rolling:
        df.index = df.index.str.replace('Rolling-', '')
    # Apaga data inicial
    if data_inicial:
        df.index = df.index.str.replace(r'\d{8}-', '', regex=True)
    # Apaga percentual de fricção
    if perc_friccao:
        df.index = df.index.str.replace(r'\d+%-', '', regex=True)
    # Apaga nome de usinas
    if nome_usinas:
        df.index = df.index.str.replace('(GBM-)'
                                        '|(FURNAS-)'
                                        '|(SOBRADINHO-)'
                                        '|(TUCURUI-)'
                                        '|(COMP-)', '', regex=True)
    return df


def formatar_df_para_bi(df_in: pd.DataFrame) -> pd.DataFrame:
    """
    Reordena as informações para mostrar no powerbi.

    Parameters
    ----------
    df_in : DataFrame
        Dados a serem mostrados no powerbi.

    Returns
    -------
    DataFrame
        Novo formato para ser salvo em excel e mostrado no powerbi.

    """
    # Para não mudar o original informado
    df = df_in.copy()
    # colunas como string
    df.columns = [str(column) for column in df.columns]
    df.columns.name = 'mes'
    df.index.name = 'serie'

    len_columns = len(df.columns)
    # Para posteriormente criar a coluna de ordem
    ordem = list(range(1, len_columns + 1)) * 4
    # Para identificar a série
    serie_id = sorted([1, 2, 3, 4] * len_columns)

    # Insere uma coluna 'p' para agrupamento
    # TODO: verificar no split onde ficará o 'p'. Aqui presume-se que fica
    # na posição 1
    df['p'] = df.index.str.split('-').str[1].str.lower()
    df = df.reset_index().set_index(['p', 'serie']).stack(dropna=False)

    # Separa em uma lista de dataframes separado por 'p'
    df_list = []
    for p, sub_df in df.groupby('p', sort=False):

        sub_df = sub_df.reset_index()
        sub_df['serie_id'] = serie_id[:len(sub_df)]
        sub_df.set_index(['serie_id', 'mes'], inplace=True)
        sub_df.rename(columns={'serie': f'Nome da Série_{p}',
                               0: f'valor_{p}',
                               },
                      inplace=True)
        del sub_df['p']
        df_list.append(sub_df)

    # Insere também a tabela sem o 'p' (MEDIA TOTAL p. ex.)
    try:
        df_med = df.loc[np.nan].reset_index()
    except KeyError:
        pass
    else:
        df_med['serie_id'] = serie_id[:len(df_med)]
        df_med.set_index(['serie_id', 'mes'], inplace=True)
        df_med.columns = ['calc_agr', 'valor_med_total']
        df_list.append(df_med)

    # Concatena os dataframes da lista em colunas
    df_res = pd.concat(df_list, axis=1)
    # Insere dado de ordem, necessário para o powerbi se organizar
    df_res.insert(0, 'ordem', ordem[:len(df_res)])

    # Apaga informações a não serem mostradas
    df_res = df_res.replace(np.nan, '')

    return df_res.droplevel('serie_id')
