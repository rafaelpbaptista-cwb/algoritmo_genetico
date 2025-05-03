# -*- coding: utf-8 -*-
"""Conexão com a database ClimaTempo."""
from datetime import datetime, timedelta
import pandas as pd
from infra_copel.mongodb.base import MongoDatabase

class MongoClimaTempo(MongoDatabase):
    """
    Representa a conexão com a database 'climatempo' no MongoDB.

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
        super().__init__("climatempo")
        
    @property
    def ena(self) -> pd.DataFrame:
        """
        Dados de ENA disponibilizados pela climatempo nos arquivos ENAs_13meses_MMdd_revX.xlsx

        Returns
        -------
        pd.DataFrame
            DataFrame
        """
        return self.collection_to_df("ena")
