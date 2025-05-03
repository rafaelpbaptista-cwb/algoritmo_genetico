"""Módulo acesso dados collection prev_temp_ponderado_tok10"""
import pandas as pd
from datetime import datetime, timedelta
from infra_copel.mongodb.tempook import MongoTempoOK

class PrevTempPonderadoTok10DAO:


    def __init__(self):
        self.mongo = MongoTempoOK()

    def obter_prev_temperatura_ponderada_tok10_mais_recente(self) -> pd.DataFrame:
        """
        Dados de previsão da temperatura ponderada para os próximos 10 dias mais recente

        Returns
        -------
        pd.DataFrame
            Dados previsão temperatura mais recente para os próximos 10 dias
        """
        df = self.mongo.previsao_temperatura_ponderada_tok10
        
        return df[
            (df['periodo'] == df['periodo'].max()) &
            (df['data_previsao'] >= (datetime.now() - timedelta(days=2)))
        ]