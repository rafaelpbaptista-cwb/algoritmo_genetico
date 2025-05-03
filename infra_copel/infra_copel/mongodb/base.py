"""Abstração da conexão com o MongoDB, seja com config.ini ou Airflow."""

import pandas as pd
import polars as pl
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

from infra_copel.config import cfg_mongodb


def get_mongodb_client(db_name: str) -> MongoClient | None:
    """
    Retorna o MongoClient caso o servidor esteja disponível.

    Parameters
    ----------
    db_name : str
        Nome da configuração do mongo.

    Returns
    -------
    MongoClient | None

    """
    if mongodb_param := cfg_mongodb(db_name):
        # Se encontrou um arquivo config.ini (ambiente dev)
        client = MongoClient(**mongodb_param)
    else:
        # Provavelmente um ambiente Airflow
        try:
            from airflow.providers.mongo.hooks.mongo import MongoHook
        except ModuleNotFoundError:
            raise ValueError(
                "Não foi possível realizar o import. "
                "Se você está em uma máquina de desenvolvedor, verifique seu arquivo config.ini. "
                f'Tal arquivo deve conter uma seção para o database "{db_name}"'
            )

        # Considera-se que o nome do hook é igual ao da database
        client = MongoHook(db_name).get_conn()

    try:
        # Verifica se o servidor está disponível
        client.admin.command("ismaster")
    except ServerSelectionTimeoutError:
        return None

    return client


class MongoDatabase:
    """
    Classe base para outras conexões com o MongoDB.

    Attributes
    ----------
    db : pymongo.database.Database
        Database do MongoDB.
    """

    def __init__(self, db_name: str, retries: int = 3):

        for _ in range(retries):
            if client := get_mongodb_client(db_name):
                break
        else:
            # TODO: incluir o nome do database na mensagem de erro?
            raise Exception("Não foi possível conectar no banco.")

        self._client = client
        self.db = client[db_name]

    def __getitem__(self, collection: str):
        """Retorna a collection."""
        return self.db[collection]
    
    def delete_many(self, collection: str, query: dict):
        """Exclui dados da collection."""
        self.db[collection].delete_many(query)

    def collection_to_df(
        self,
        collection: str,
        query: dict | None = None,
        projection: dict | None = None,
        sort: dict | None = None,
        index_column: int | None = None,
        polars: bool = False,
    ) -> pd.DataFrame | pl.LazyFrame:
        """
        Geração de um dataframe a partir de uma collection do banco.

        Parameters
        ----------
        collection : str
            Collection a ser tabulada.
        query
            Filtragem de valores
        projection

        index_column
            Coluna(s) a ser(em) utilizada(s) como índice.

        sort
            Ordenação dos dados de retorno.

        polars
            Retorna um LazyFrame do polars ao invés do DataFrame do pandas

        Returns
        -------
        pd.DataFrame ou pl.LazyFrame
        """
        
        if projection is None:
            # Geralmente não é necessário o id do campo no dataframe
            projection = {"_id": 0}

        # Cursor da consulta
        cursor = self.db[collection].find(
            filter=query, projection=projection, sort=sort
        )

        if polars:
            return pl.LazyFrame(cursor)
        else:
            df = pd.DataFrame(cursor)

            if not df.empty and index_column:
                df.set_index(index_column, inplace=True)

            return df

    def is_collection_empty(self, nome_collection: str) -> bool:
        """
        Verifica se a collection está vazia

        Parameters
        ----------
        nome_collection : str
            Nome da collection que vamos verificar

        Returns
        -------
        bool
            True se a collection estiver vazia e False caso contrário
        """
        return len(list(self[nome_collection].find({}).limit(1))) == 0
