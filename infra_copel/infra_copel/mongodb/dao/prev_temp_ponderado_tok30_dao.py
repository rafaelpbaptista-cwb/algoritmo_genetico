"""Classe DAO (Data Access Object) para a collection prev_temp_ponderado_tok30"""
import pandas as pd
from datetime import datetime
from infra_copel.mongodb.tempook import MongoTempoOK

class PrevTempPonderadoTok30DAO:

    def __init__(self):
        self.mongo = MongoTempoOK()

    def obter_prev_temperatura_ponderada_tok30_mais_recente(self, data_previsao: datetime) -> pd.DataFrame:
        """
        Dados de previsão da temperatura ponderada para os próximos 30 dias mais recente

        Parameters
        ----------
        data_previsao : datetime
            Obter registros cuja data_previsão seja maior que a informada

        Returns
        -------
        pd.DataFrame
            Dados previsão temperatura mais recente para os próximos 10 dias
        """
        df = self.mongo.previsao_temperatura_ponderada_tok30
        
        return df[(df['periodo'] == df['periodo'].max()) & (df['data_previsao'] >= data_previsao)]