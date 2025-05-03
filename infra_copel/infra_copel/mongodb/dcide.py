# -*- coding: utf-8 -*-
"""Conexão com a database dcide."""
import pandas as pd
import pymongo

from pymongo.errors import BulkWriteError
from pymongo.command_cursor import CommandCursor
from datetime import datetime
from infra_copel.mongodb.base import MongoDatabase
from infra_copel.mongodb.util import df_to_docs

COLLECTION_DCIDE_CALENDARIO = "dcide_calendario"
COLLECTION_FORWARD_COMPETITIVA = "forward_competitiva_se"


class MongoDcide(MongoDatabase):
    """
    Representa a conexão com a database 'dcide' no MongoDB.

    Attributes
    ----------
    db : MongoClient
        Cliente do MongoDB.

    """

    default_collections = [
        COLLECTION_FORWARD_COMPETITIVA,
        "forward_incentivada50_se",
        "submercado_s",
        "submercado_ne",
        "submercado_n",
    ]

    def __init__(self):
        """
        Construtor da classe.

        Cria o objeto baseado no nome da database.
        """
        super().__init__("dcide")

    @property
    def df_dcide_calendario(self) -> pd.DataFrame:
        """
        Dataframe com os dados dcide_calendario

        Segue descrição dos campos:
            - semana (int): Semana do ano
            - data_ini_vig (Date): Data que o preço do produto foi disponibilizado. A data que a informação foi liberada
            - valor_produto (float): Valor do produto dcide A+N
            - ano_produto (int): Ano do produto dependendo do produto (A+1, A+2, A+3 ou A+4)
            - valor_atualizado_ipca (float): Valor do produto corrigido pelo IPCA para os valores presentes
        """
        return self.collection_to_df(COLLECTION_DCIDE_CALENDARIO)

    @property
    def df_forward_competitiva_se(self) -> pd.DataFrame:
        """Dataframe com os dados forward_competitiva_se."""
        df = self.collection_to_df(
            "forward_competitiva_se",
            index_column=["data_ini_vig", "maturidade", "legenda"],
        )
        return df

    def get_valor_dcide_calendario(
        self, data_inicio_vigencia: str, ano_produto: int
    ) -> pd.DataFrame:
        """
        Obtem o valor do dcide na data vigência e ano produto informado

        Parameters
        ----------
        data_inicio_vigencia : str
            Data no formato YYYY-MM
        ano_produto : int
            Ano produto

        Returns
        -------
        pd.DataFrame
            Dataframe contendo a média do mês solicitado. Obs: Os dados da dcide são semanais
        """
        resultado = self["dcide_calendario"].aggregate(
            [
                {
                    "$project": {
                        "_id": False,
                        "valor_produto": True,
                        "ano_produto": True,
                        "periodo": {
                            "$dateToString": {
                                "format": "%Y-%m",
                                "date": "$data_ini_vig",
                            }
                        },
                    }
                },
                {
                    "$group": {
                        "_id": {"ano_produto": "$ano_produto", "periodo": "$periodo"},
                        "valor_produto": {"$avg": "$valor_produto"},
                    }
                },
                {
                    "$match": {
                        "_id.periodo": data_inicio_vigencia,
                        "_id.ano_produto": ano_produto,
                    }
                },
                {
                    "$project": {
                        "_id": False,
                        "periodo": "$_id.periodo",
                        "ano_produto": "$_id.ano_produto",
                        "valor_produto": True,
                    }
                },
            ]
        )

        return pd.DataFrame(list(resultado))

    def get_valor_recente_dcide_calendario(self, ano_produto: int) -> float:
        """
        Obtem o dado mais recente da dcide de acordo com o ano_produto informado como parâmetro

        Parameters
        ----------
        ano_produto : int
            Ano produto a ser utilizado n filtro

        Returns
        -------
        float
            Valor do dcide mais recente para o ano informado
        """
        resultado = (
            self[COLLECTION_DCIDE_CALENDARIO]
            .find({"ano_produto": ano_produto})
            .sort("data_ini_vig", pymongo.DESCENDING)
            .limit(1)
        )

        for dado in resultado:
            return dado["valor_produto"]

    def _esvaziar(self):
        # Checa para evitar apagar sem querer
        confirm = input(
            "Digite APAGAR para confirmar as collections da "
            "database serão esvaziadas: "
        )
        if confirm != "APAGAR":
            print("Não confirmado. Database não será esvaziada.")
            return False
        for collection in self.db.list_collection_names():
            self[collection].delete_many({})

    def popular_mongo_dcide(self, df_dcide: pd.DataFrame):
        """Popula as collections da database dcide com os dados informados."""
        for (fonte_energia, data_ini_vig), df_g in df_dcide.groupby(
            ["fonte_energia", "data_ini_vig"]
        ):
            df_g.reset_index(inplace=True)
            df_g.drop("fonte_energia", axis=1, inplace=True)
            docs = df_to_docs(df_g)
            try:
                self[fonte_energia].insert_many(docs)
            except BulkWriteError:
                print(f"{fonte_energia} {data_ini_vig} já deve existir.")

    def criar_unique_key_forward_competitiva_se(self):
        """
        Cria um indíce único na collection forward_competitiva_se para evitar duplicidade
        """

        self[COLLECTION_FORWARD_COMPETITIVA].create_index(
            [("data_ini_vig", pymongo.ASCENDING), ("maturidade", pymongo.ASCENDING)],
            unique=True,
        )

    def get_ultima_data_registro(self) -> datetime:
        """
        Obtêm a data do último registro inserido na base de dados

        Returns
        -------
        datetime
            Data do último registro
        """
        resultado = self[COLLECTION_FORWARD_COMPETITIVA].aggregate(
            [{"$group": {"_id": None, "ultima_data": {"$max": "$data_fim_vig"}}}]
        )

        return resultado.next()["ultima_data"]

    def limpar_dcice_calendario(self) -> None:
        """
        Apaga todos os dados da collection dcide_calendario
        """
        self[COLLECTION_DCIDE_CALENDARIO].delete_many({})

    def get_dados_insercao_dcice_calendario(
        self, ano_base: int, ano_produto: int
    ) -> CommandCursor:
        """
        Obtem os dados a serem usados para popular a collection produto dcide A+1
        É realizado a média ponderada para o calculo dos valores dos produtos DCIDE

        Parameters
        ----------
        ano_base : int
            Ano base que estamos realizando a obtenção dos dados
        ano_produdo : int
            Ano do produto (A+1, A+2, A+3 ou A+4)

        Returns
        -------
        CommandCursor
            resultado da pesquisa a ser usado para popular a collection dcide_calendario
        """

        resultado = self["forward_competitiva_se"].aggregate(
            [
                {
                    "$project": {
                        "_id": False,
                        "semana": True,
                        "ano": {"$year": "$data_fim_vig"},
                        "data": "$data_ini_vig",
                        "mpd": True,
                        "legenda": True,
                        # Lógica par saber qual peso usar para o cálculo da média ponderada
                        "peso": {
                            "$cond": {
                                "if": {
                                    "$or": [
                                        {
                                            "$regexMatch": {
                                                "input": "$legenda",
                                                "regex": "^01",
                                            }
                                        },
                                        {
                                            "$and": [
                                                {
                                                    "$regexMatch": {
                                                        "input": "$legenda",
                                                        "regex": "^02",
                                                    }
                                                },
                                                {
                                                    "$regexMatch": {
                                                        "input": "$legenda",
                                                        "regex": f"{ano_produto}$",
                                                    }
                                                },
                                            ]
                                        },
                                        {
                                            "$and": [
                                                {
                                                    "$regexMatch": {
                                                        "input": "$legenda",
                                                        "regex": "^03",
                                                    }
                                                },
                                                {
                                                    "$regexMatch": {
                                                        "input": "$legenda",
                                                        "regex": f"{ano_produto}$",
                                                    }
                                                },
                                            ]
                                        },
                                        {
                                            "$and": [
                                                {
                                                    "$regexMatch": {
                                                        "input": "$legenda",
                                                        "regex": "^04",
                                                    }
                                                },
                                                {
                                                    "$regexMatch": {
                                                        "input": "$legenda",
                                                        "regex": f"{ano_produto}$",
                                                    }
                                                },
                                            ]
                                        },
                                    ]
                                },
                                "then": 1,
                                "else": {
                                    "$cond": {
                                        "if": {
                                            "$regexMatch": {
                                                "input": "$legenda",
                                                "regex": "^02",
                                            }
                                        },
                                        "then": 2,
                                        "else": {
                                            "$cond": {
                                                "if": {
                                                    "$regexMatch": {
                                                        "input": "$legenda",
                                                        "regex": "^03",
                                                    }
                                                },
                                                "then": 3,
                                                "else": 0,
                                            }
                                        },
                                    }
                                },
                            }
                        },
                    }
                },
                {"$match": {"ano": ano_base, "legenda": {"$regex": str(ano_produto)}}},
                {
                    "$group": {
                        "_id": {"semana": "$semana", "ano": "$ano", "data": "$data"},
                        "itens": {"$addToSet": {"mpd": "$mpd", "peso": "$peso"}},
                        "qtdade_grupo": {"$count": {}},
                    }
                },
                {"$unwind": "$itens"},
                {
                    "$project": {
                        "_id": False,
                        "semana": "$_id.semana",
                        "ano": "$_id.ano",
                        "data": "$_id.data",
                        "mpd": "$itens.mpd",
                        "peso": "$itens.peso",
                        "qtdade_grupo": True,
                        "mpd_calculado": {
                            "$cond": {
                                #  Valor mensal a ser calculado o peso usado no calculo
                                "if": {"$gt": ["$itens.peso", 0]},
                                "then": {"$multiply": ["$itens.mpd", "$itens.peso"]},
                                # Valor médio para o ano
                                "else": {
                                    "$multiply": [
                                        "$itens.mpd",
                                        {
                                            "$subtract": [
                                                12,
                                                {"$subtract": ["$qtdade_grupo", 1]},
                                            ]
                                        },
                                    ]
                                },
                            }
                        },
                    }
                },
                {
                    "$group": {
                        "_id": {
                            "semana": "$semana",
                            "ano": "$ano",
                            "data": "$data",
                            "qtdade_grupo": "$qtdade_grupo",
                        },
                        "numerador": {"$sum": "$mpd_calculado"},
                    }
                },
                {
                    "$project": {
                        "_id": False,
                        "semana": "$_id.semana",
                        "data_ini_vig": "$_id.data",
                        "valor_produto": {"$divide": ["$numerador", 12]},
                    }
                },
                {"$addFields": {"ano_produto": ano_produto}},
                {"$sort": {"semana": 1}},
            ]
        )

        return resultado
