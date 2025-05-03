# -*- coding: utf-8 -*-
"""Conexão com a database historico_oficial."""
from datetime import datetime
from dateutil.relativedelta import relativedelta
import logging

import pandas as pd

from infra_copel.mongodb.base import MongoDatabase


class MongoHistoricoOficial(MongoDatabase):
    """
    Classe que representa conexão com o 'historico_oficial' no MongoDB.

    Attributes
    ----------
    db : MongoClient
        Cliente do MongoDB.
    """

    def __init__(self):
        """
        Construtor da classe.

        Cria o objeto baseado no nome da database.
        """
        super().__init__('historico_oficial')

    @property
    def df_populacao_estado(self) -> pd.DataFrame:
        """
        Dados população dos estados do Brasil
        Informação populada manualmente (Data: 17/10/2023)
        """
        return self.collection_to_df('populacao_estado')
    
    @property
    def df_ipca(self) -> pd.DataFrame:
        """
        Histórico de carga do Sintegre que inclui dados da carga de usinas não despachadas centralizadamente
        """
        return self.collection_to_df('ipca', projection = {'_id': True, "num_indice": True})

    @property
    def df_ons_balanco_energia_dessem(self) -> pd.DataFrame:
        """
        Balanço de Energia do DESSEM.
        """
        df = self.collection_to_df('ons_balanco_energia_dessem',
                                   sort=[('periodo', 1), ('submercado', -1)])
        df['periodo'] = pd.PeriodIndex(df['periodo'], freq='30T')
        df.set_index(['periodo','submercado'], inplace=True)
        return df

    @property
    def df_ons_balanco_programacao_diaria(self) -> pd.DataFrame:
        """
        Balanço da Programação Diária.
        """
        df = self.collection_to_df('ons_balanco_programacao_diaria',
                                   sort=[('periodo', 1), ('submercado', -1)])
        df['periodo'] = pd.PeriodIndex(df['periodo'], freq='30T')
        df.set_index(['periodo','submercado'], inplace=True)
        return df

    @property
    def df_ons_carga_diaria(self) -> pd.DataFrame:
        """
        Carga, em MWMed, do sistema agrupado de forma diária.
        Esses dados estão consolidados e possui uma defasagem de 1 dia.

        Obs: 
        - Até fevereiro/2021, os dados representam a carga atendida por usinas despachadas e/ou programadas pelo ONS, 
        mas não com base em dados recebidos pelo Sistema de Supervisão e Controle do ONS. 
        
        - Entre março/2021 e abril/23, os dados representam a carga atendida por usinas despachadas e/ou programadas pelo ONS, 
         com base em dados recebidos pelo Sistema de Supervisão e Controle do ONS, mais a previsão de geração de usinas não despachadas pelo ONS. 
         
         - A partir de 29/04/2023, além dos dados anteriormente considerados, passou a ser incorporado o valor estimado da micro e minigeração distribuída (MMGD), 
         com base em dados meteorológicos previstos.
        """
        return self.collection_to_df('ons_carga_diaria')

    @property
    def df_ons_carga_mensal(self) -> pd.DataFrame:
        """
        Carga, em MWMed, do sistema agrupado de forma mensal.
        Esses dados estão consolidados e possui uma defasagem de 1 mês
        """
        return self.collection_to_df('ons_carga_mensal')

    @property
    def df_ena_mlt(self) -> pd.DataFrame:
        """
        MLT da ENA utilizada no mês.
        É a Media de Longo Termo da ENA para cada mês do ano
        """
        df = self.collection_to_df('ena_mlt',
                                   sort=[('mes', 1)],
                                   index_column='mes')
        df.index = pd.PeriodIndex(df.index, freq='M')
        df.index.name = 'periodo'
        df.columns.name = 'submercado'
        return df
    
    @property
    def df_ons_taxa_teif_teip(self) -> pd.DataFrame:
        """
        Taxas TEIFa (Taxa Equivalente de Indisponibilidade Forçada) e TEIP(Taxa de Indisponibilidade Programada)
        das usinas despachadas centralizadamente pelo ONS com discretização mensal.
        Obs:
        - Dados a partir de out/2018.
        """
        df = self.collection_to_df('taxas_teif_teip',
                                   sort=[('periodo', 1)],
                                   index_column='periodo')
        df.index = pd.PeriodIndex(df.index, freq='M')
        df.index.name = 'periodo'
        return df

    @property
    def df_ear_diario_reservatorio(self) -> pd.DataFrame:
        """EAR diária por submercado."""
        df = self.collection_to_df('ons_ear_diario_reservatorio',
                                   sort=[('periodo', 1), ('submercado', -1)])
        df['periodo'] = pd.PeriodIndex(df['periodo'], freq='D')
        df.set_index(['periodo','submercado'], inplace=True)
        return df

    @property
    def df_ear_submercado(self) -> pd.DataFrame:
        """EAR diária por submercado."""
        df = self.collection_to_df('ons_ear_subsistema',
                                   sort=[('periodo', 1), ('submercado', -1)])
        df['periodo'] = pd.PeriodIndex(df['periodo'], freq='D')
        df.set_index(['periodo','submercado'], inplace=True)
        return df

    @property
    def df_ena_submercado(self) -> pd.DataFrame:
        """ENA diária por subsistema."""
        df = self.collection_to_df('ons_ena_subsistema',
                                   sort=[('periodo', 1), ('submercado', -1)])
        df['periodo'] = pd.PeriodIndex(df['periodo'], freq='D')
        df.set_index(['periodo','submercado'], inplace=True)
        return df

    @property
    def df_fator_sazo(self) -> pd.DataFrame:
        """Fator de Sazonalização utilizado no mês."""
        df = self.collection_to_df('fator_sazonalizacao_gf_mre',
                                   sort=[('mes', 1)],
                                   index_column='mes')
        df.index = pd.PeriodIndex(df.index, freq='M')
        df.index.name = 'periodo'
        return df

    @property
    def df_limites_pld_tarifas(self) -> pd.DataFrame:
        """
        Limites de PLD e tarifas anuais.
        Descrição das colunas:
        ano	Double	Ano relativo aos valores
            - PLD_MIN:	Double	Valor mínimo que o PLD pode ter por hora, em R$/MWh
            - PLD_MAX_EST:	Double	Valor médio máximo que o PLD pode ter por dia, em R$/MWh
            - PLD_MAX_H:	Double	Valor máximo que o PLD pode ter por hora, em R$/MWh
            - PLD_X:	Double	Valor associado ao custo de oportunidade de geração em razão do armazenamento incremental dos reservatórios decorrento de deslocamento hidráulico, em R$/MWh
            - custo_deficit:	Double	Valor atribuído ao déficit de energia, em R$/MWh
            - TEO:	Double	Tarifa de Energia de Otimização, em R$/MWh
            - TEOItaipu:	Double	Tarifa de Energia de Otimização de Itaipu, em R$/MWh
            - TSA:	Double	Tarifa de Serviços Ancilares, em R$/MVArh
        """
        df = self.collection_to_df('limites_pld_tarifas_anuais',
                                   sort=[('ano', 1)],
                                   index_column='ano')
        df.index = pd.PeriodIndex(df.index, freq='Y')
        df.index.name = 'periodo'
        return df

    @property
    def df_meses(self) -> pd.DataFrame:
        """Dados com a quantidade de horas e dias no mês."""
        df = self.collection_to_df('meses',
                                   sort=[('mes', 1)],
                                   index_column='mes')
        df.index = pd.PeriodIndex(df.index, freq='M')
        df.index.name = 'periodo'
        return df

    @property
    def df_mre_historico(self) -> pd.DataFrame:
        """Dados do MRE oriundos da CCEE."""
        df = self.collection_to_df('mre_historico',
                                   sort=[('mes', 1)],
                                   index_column='mes')
        df.index = pd.PeriodIndex(df.index, freq='M')
        df.index.name = 'periodo'
        return df

    @property
    def df_pld_horario(self) -> pd.DataFrame:
        """PLD horário por submercado."""
        df = self.collection_to_df('pld_horario',
                                   sort=[('hora', 1)],
                                   index_column='hora')
        df.index = pd.PeriodIndex(df.index, freq='H')
        df.index.name = 'periodo'
        return df

    @property
    def df_pld_semanal(self) -> pd.DataFrame:
        """PLD médio semanal por submercado."""
        df = self.collection_to_df('view_pld_semanal', index_column='hora_inicial')
        df.drop('hora_final', axis=1, inplace=True)
        df.index = pd.PeriodIndex(df.index, freq='W-FRI')
        df.index.name = 'periodo'
        return df

    @property
    def df_pld_mensal(self) -> pd.DataFrame:
        """PLD médio mensal por submercado."""
        df = self.collection_to_df('view_pld_mensal', index_column='mes')
        df.index = pd.PeriodIndex(df.index, freq='M')
        df.index.name = 'periodo'
        return df

    @property
    def df_pld_anual(self) -> pd.DataFrame:
        """PLD médio anual por submercado."""
        df = self.collection_to_df('view_pld_anual', index_column='ano')
        df.index = pd.PeriodIndex(df.index, freq='Y')
        df.index.name = 'periodo'
        return df

    @property
    def fator_sazonalizador_gf(self) -> pd.DataFrame:
        """Fator a ser multiplicado a GF para achar a GF sazonalizada.

        O fator é calculado pois a CCEE divulga o fator_sazo que
        corresponde à participação do mês referente ao ano.
        Também pode ser usado para

        """
        fator_sazo = self.df_fator_sazo['fator_sazo']
        horas_mes = self.df_meses['qtde_horas']
        horas_ano = horas_mes.resample('Y').sum().resample('M').ffill()

        return (fator_sazo * horas_ano / horas_mes).loc[fator_sazo.index]

    def calcular_preco_atualizado_ipca(self, valor: float, data_lancamento_valor: datetime, data_correcao: datetime) -> float:
        """
        Faz a correção do valor na data de lançamento para a data de correção. É aplicado a correção usando o indice IPCA.
        No cálculo é considerado o seguinte intervalo: data preço - 1 mês até data correção - 1 mês

        Parameters
        ----------
        valor : float
            Valor a ser corrigido
        data : datetime
            Data que o valor foi lançado

        Returns
        -------
        float
            Valor corrigido
        """

        data_pesquisa_passado = data_lancamento_valor.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        data_pesquisa_passado = data_pesquisa_passado - relativedelta(months=1)

        data_pesquisa_recente = data_correcao.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        data_pesquisa_recente = data_pesquisa_recente - relativedelta(months=1)

        ipca_passado = self['ipca'].find_one({'_id': data_pesquisa_passado})
        ipca_recente = self['ipca'].find_one({'_id': data_pesquisa_recente})

        if ipca_passado is not None and ipca_recente is not None:
            valor_corrigido = valor * (ipca_recente['num_indice'] / ipca_passado['num_indice'])
            logging.debug('Calculando ajustes no valor. Valor antigo: %s, Valor novo: %s, data lancamento: %s, data correção: %s', valor, valor_corrigido, data_lancamento_valor, data_correcao)

            return valor_corrigido
        else:
            logging.info('Valor não foi corrigido para a data de lançamento: %s e data de correção: %s', data_lancamento_valor, data_correcao)
            return valor

    @property
    def df_ons_balanco_energia_subsistema(self) -> pd.DataFrame:
        """
        Balanço de Energia por subsitema.
        """
        df = self.collection_to_df('ons_balanco_energia_subsistema',
                                   sort=[('periodo', 1), ('submercado', -1)])
        df['periodo'] = pd.PeriodIndex(df['periodo'], freq='30T')
        df.set_index(['periodo','submercado'], inplace=True)
        return df
