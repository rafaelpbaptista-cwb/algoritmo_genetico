# -*- coding: utf-8 -*-
"""Conexão com a database TempoOk."""
from datetime import datetime, timedelta
import pandas as pd
from infra_copel.mongodb.base import MongoDatabase


class MongoTempoOK(MongoDatabase):
    """
    Representa a conexão com a database 'tempook' no MongoDB.

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
        super().__init__("tempook")

    @property
    def prev_geracao_eolica_tok30(self) -> pd.DataFrame:
        """
        Previsão de geração de energia eólica (MWm) para os próximos 30 dias.
        Essa previsão vem em base horária por submercado

        Returns
        -------
        pd.DataFrame
            DataFrame
        """
        return self.collection_to_df("prev_geracao_eolica_tok30")

    @property
    def historico_temperatura_ponderada(self) -> pd.DataFrame:
        """
        Dados da temperatura histórica ponderada pela quantidade população

        Returns
        -------
        pd.DataFrame
            DataFrame
        """
        return self.collection_to_df("hist_temp_ponderado")

    @property
    def prev_geracao_eolica_tok10(self) -> pd.DataFrame:
        """
        Dados da temperatura histórica ponderada pela quantidade população

        Returns
        -------
        pd.DataFrame
            DataFrame
        """
        return self.collection_to_df("prev_geracao_eolica_tok10")

    @property
    def previsao_temperatura_ponderada_tok10(self) -> pd.DataFrame:
        """
        Dados de previsão da temperatura ponderada para os próximos 10 dias

        Returns
        -------
        pd.DataFrame
            DataFrame
        """

        return self.collection_to_df("prev_temp_ponderado_tok10")

    @property
    def previsao_temperatura_ponderada_tok30(self) -> pd.DataFrame:
        """
        Dados de previsão da temperatura ponderada para os próximos 30 dias

        Returns
        -------
        pd.DataFrame
            DataFrame
        """

        return self.collection_to_df("prev_temp_ponderado_tok30")

    @property
    def ecens45_m(self) -> pd.DataFrame:
        """
        Dados de ENA de um membro do European Center Ensemble 45 days per member.

        Returns
        -------
        pd.DataFrame
            DataFrame
        """

        return self.collection_to_df("ecens45_m")

    @property
    def ecens_m(self) -> pd.DataFrame:
        """
        Dados de ENA de um membro do European Center Ensemble per member.

        Returns
        -------
        pd.DataFrame
            DataFrame
        """

        return self.collection_to_df("ecens_m")

    @property
    def ecenslt_m(self) -> pd.DataFrame:
        """
        Dados de ENA de um membro do European Center Ensemble Long Term per member.

        Returns
        -------
        pd.DataFrame
            DataFrame
        """

        return self.collection_to_df("ecenslt_m")

    @property
    def ecenslt_estat(self) -> pd.DataFrame:
        """
        Dados de ENA de um membro do European Center Ensemble Long Term estatísticas.

        Returns
        -------
        pd.DataFrame
            DataFrame
        """

        return self.collection_to_df("ecenslt_estat")
    
    @property
    def prev_geracao_eolica_tok_ia(self) -> pd.DataFrame:
        """
        Dados de energia eolica de TOK-IA.

        Returns
        -------
        pd.DataFrame
            DataFrame
        """

        return self.collection_to_df("prev_geracao_eolica_tokia")
    
    @property
    def enas_geradas_tok(self) -> pd.DataFrame:
        """
        Dados de ena (mlt %) geradas por cenario na TempoOk 

        Returns
        -------
        pd.DataFrame
            DataFrame
        """

        return self.collection_to_df("enas_mlt_tok")