# -*- coding: utf-8 -*-
"""Conexão com a database rodadas."""
import pandas as pd
from infra_copel.mongodb.base import MongoDatabase


class MongoBBCE(MongoDatabase):
    """
    Classe que representa conexão com 'BBCE' no MongoDB.

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
        super().__init__("bbce")

    @property
    def df_preco_tickers(self) -> pd.DataFrame:
        """
        Collection contendo preços diários de ativos(produtos) negociados na BBCE.

        Returns
        -------
        pd.DataFrame
            DataFrame
        """
        return self.collection_to_df(collection="preco_tickers")
