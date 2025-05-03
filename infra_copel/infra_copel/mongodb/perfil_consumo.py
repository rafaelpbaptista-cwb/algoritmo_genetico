import polars as pl
import pandas as pd
from datetime import datetime
from infra_copel.mongodb.base import MongoDatabase
from pymongo import UpdateOne
from tqdm import tqdm


class PerfilConsumo(MongoDatabase):
    """
    Classe que representa uma conexão com o Database 'perfil_consumo'

    Parameters
    ----------
    MongoDatabase : MongoDatabase
        Classe mãe
    """

    def __init__(self):
        """
        Construtor da classe.

        Cria o objeto baseado no nome da database.
        """
        super().__init__("perfil_consumo")

    @property
    def df_consumo_horario(self) -> pl.LazyFrame:
        """
        Dados da collection consumo_horario no formato pl.LazyFrame

        Returns
        -------
        pl.LazyFrame
            Todos os dados da collection dentro do LazyFrame
        """
        return self.collection_to_df(collection="consumo_horario", polars=True)

    @property
    def df_media_consumo_final_semana(self) -> pl.LazyFrame:
        """
        Dados da collection media_consumo_final_semana no formato pl.LazyFrame

        Returns
        -------
        pl.LazyFrame
            Todos os dados da collection dentro do LazyFrame
        """
        return self.collection_to_df(
            collection="media_consumo_final_semana", polars=True
        )

    @property
    def df_media_consumo_dia_noite(self) -> pl.LazyFrame:
        """
        Dados da collection media_consumo_dia_noite no formato pl.LazyFrame

        Returns
        -------
        pl.LazyFrame
            Todos os dados da collection dentro do LazyFrame
        """
        return self.collection_to_df(collection="media_consumo_dia_noite", polars=True)

    def upsert_many_media_consumo_final_semana(self, lista_docs: list) -> None:
        """
        Realiza o insert ou o update dos itens contidos na lista_docs na media_consumo_final_semana

        Parameters
        ----------
        collection : str
            Collection que receberá os dados
        lista_docs : list
            lista de dicionários contendo os dados a serem inseridos ou atualizados
        """

        operacoes = [
            UpdateOne(
                filter={"CODIGO_CARGA": item["CODIGO_CARGA"]},
                update={"$set": item},
                upsert=True,
            )
            for item in lista_docs
        ]

        with tqdm(
            total=len(operacoes), desc="Carga media_consumo_final_semana"
        ) as pbar:
            for i in range(0, len(operacoes), 100):
                batch = operacoes[i : i + 100]
                self.db["media_consumo_final_semana"].bulk_write(batch)
                pbar.update(len(batch))

    def upsert_many_media_consumo_dia_noite(self, lista_docs: list) -> None:
        """
        Realiza o insert ou o update dos itens contidos na lista_docs na collection em questão

        Parameters
        ----------
        collection : str
            Collection que receberá os dados
        lista_docs : list
            lista de dicionários contendo os dados a serem inseridos ou atualizados
        """

        operacoes = [
            UpdateOne(
                filter={"CODIGO_CARGA": item["CODIGO_CARGA"]},
                update={"$set": item},
                upsert=True,
            )
            for item in lista_docs
        ]

        with tqdm(total=len(operacoes), desc="Carga media_consumo_dia_noite") as pbar:
            for i in range(0, len(operacoes), 100):
                batch = operacoes[i : i + 100]
                self.db["media_consumo_dia_noite"].bulk_write(batch)
                pbar.update(len(batch))

    def upsert_many_consumo_horario(self, lista_docs: list) -> None:
        """
        Realiza o insert ou o update dos itens contidos na lista_docs na collection em questão

        Parameters
        ----------
        lista_docs : list
            lista de dicionários contendo os dados a serem inseridos ou atualizados
        """
        operacoes = [
            UpdateOne(
                filter={
                    "CODIGO_CARGA": item["CODIGO_CARGA"],
                    "DATA_HORA": item["DATA_HORA"],
                },
                update={"$set": item},
                upsert=True,
            )
            for item in lista_docs
        ]

        tamanho_batch = 10000
        for i in range(0, len(operacoes), tamanho_batch):
            batch = operacoes[i : i + tamanho_batch]
            self.db["consumo_horario"].bulk_write(batch)

    def buscar_melhores_perfis_consumo_dia_noite(
        self, limit: int = None, percent_limit_min_max: float = 3.0
    ) -> pl.DataFrame:
        """
        Obtêm os melhores perfis de consumo de acordo com a proporção MWm_dia e MWm_noite

        Parameters
        ----------
        limit : int, optional
            Limite de itens a serem retornados, by default None

        percent_limit_min_max: float, optional
            Limite percentual entre a menor proporção de MMw_dia e MMw_noite, by default 1.5

        Returns
        -------
        pl.DataFrame
            Dataframe com os melhores perfis de consumo
        """

        dicionario_match = {}
        lista_add_fields = []
        for data in pd.period_range(
            start=self.get_data_consumo_horario(),
            end=self.get_data_consumo_horario(False),
            freq="M",
        ):
            data_formatada = data.strftime("%Y%m")

            dicionario_match[f"PERCENT_DIA{data_formatada}"] = {"$exists": True}

            lista_add_fields.append(f"$PERCENT_DIA{data_formatada}")

        pipeline = [
            {"$match": dicionario_match},
            {"$addFields": {"percentValues": lista_add_fields}},
            {
                "$addFields": {
                    "stdDevPercent": {"$stdDevPop": "$percentValues"},
                    "percent_min": {"$min": "$percentValues"},
                    "percent_max": {"$max": "$percentValues"},
                    "percent_soma": {"$sum": "$percentValues"},
                    "percent_medio": {"$avg": "$percentValues"},
                }
            },
            {
                "$match": {
                    "$and": [
                        {
                            "$expr": {
                                "$lt": [
                                    "$percent_max",
                                    {
                                        "$multiply": [
                                            "$percent_min",
                                            percent_limit_min_max,
                                        ]
                                    },
                                ]
                            }
                        },
                        {"$expr": {"$gt": ["$percent_medio", 1]}},
                    ]
                }
            },
            {
                "$setWindowFields": {
                    "sortBy": {"percent_soma": -1},
                    "output": {"colocacao": {"$rank": {}}},
                }
            },
            {"$project": {"_id": False, "percentValues": False}},
        ]

        if limit is not None:
            pipeline.append({"$limit": limit})

        return pl.DataFrame(
            list(self.db["media_consumo_dia_noite"].aggregate(pipeline))
        )

    def buscar_melhores_perfis_consumo_final_semana(
        self,
        limit: int = None,
        percent_limit_min_max: float = 3.0,
        percent_limit_medio: float = 0.25,
    ) -> pl.DataFrame:
        """
        Obtêm os melhores perfis de consumo de acordo com a proporção final semana e dia semana

        Parameters
        ----------
        limit : int, optional
            Limite de itens a serem retornados, by default None

        percent_limit_min_max: float, optional
            Limite percentual entre a menor proporção de MMw_dia e MMw_noite, by default 1.5

        percent_limit_medio: float, optional
            Limite percentual entre o consumo médio dos dias úteis comparado com o consumo do final de semana, by default 0.25

        Returns
        -------
        pl.DataFrame
            Dataframe com os melhores perfis de consumo
        """
        dicionario_match = {}
        lista_add_fields = []
        for data in pd.period_range(
            start=self.get_data_consumo_horario(),
            end=self.get_data_consumo_horario(False),
            freq="M",
        ):
            data_formatada = data.strftime("%Y%m")

            dicionario_match[f"PERCENT_FINAL_SEMANA{data_formatada}"] = {
                "$exists": True
            }

            lista_add_fields.append(f"$PERCENT_FINAL_SEMANA{data_formatada}")

        pipeline = [
            {"$match": dicionario_match},
            {"$addFields": {"percentValues": lista_add_fields}},
            {
                "$addFields": {
                    "stdDevPercent": {"$stdDevPop": "$percentValues"},
                    "percent_min": {"$min": "$percentValues"},
                    "percent_max": {"$max": "$percentValues"},
                    "percent_medio": {"$avg": "$percentValues"},
                }
            },
            {
                "$match": {
                    "$and": [
                        {
                            "$expr": {
                                "$lt": [
                                    "$percent_max",
                                    {
                                        "$multiply": [
                                            "$percent_min",
                                            percent_limit_min_max,
                                        ]
                                    },
                                ]
                            }
                        },
                        {"$expr": {"$gt": ["$percent_medio", percent_limit_medio]}},
                    ]
                }
            },
            {
                "$setWindowFields": {
                    "sortBy": {"percent_medio": -1},
                    "output": {"colocacao": {"$rank": {}}},
                }
            },
            {"$project": {"_id": False, "percentValues": False}},
        ]

        if limit is not None:
            pipeline.append({"$limit": limit})

        return pl.DataFrame(
            list(self.db["media_consumo_final_semana"].aggregate(pipeline))
        )

    def buscar_colocacao_consumo_horario(
        self, lista_cod_carga: list[int]
    ) -> pl.LazyFrame:
        return pl.LazyFrame(
            (
                doc
                for doc in self["consumo_horario"].aggregate(
                    [
                        {"$match": {"CODIGO_CARGA": {"$in": lista_cod_carga}}},
                        {
                            "$group": {
                                "_id": "$CODIGO_CARGA",
                                "colocacao_dia_noite": {
                                    "$first": "$colocacao_dia_noite"
                                },
                                "colocacao_final_semana": {
                                    "$first": "$colocacao_final_semana"
                                },
                                "MWm_periodo": {"$avg": "$CONSUMO_CARGA_AJUSTADO"},
                            }
                        },
                        {
                            "$project": {
                                "_id": False,
                                "CODIGO_CARGA": "$_id",
                                "colocacao_dia_noite": True,
                                "colocacao_final_semana": True,
                                "MWm_periodo": True,
                            }
                        },
                    ]
                )
            )
        )

    def buscar_data_consumo_horario(self, menor: bool = True) -> datetime:
        """
        Obtêm a menor ou a maior data registrada na collection consumo_horario

        Parameters
        ----------
        menor : bool, optional
            Se true obtem a menor data, caso contrário a maior, by default True

        Returns
        -------
        datetime
            Data e hora mínima ou máxima
        """

        if menor:
            pesquisa = [{"$sort": {"DATA_HORA": 1}}]

        else:
            pesquisa = [{"$sort": {"DATA_HORA": -1}}]

        pesquisa.extend(
            [
                {"$limit": 1},
                {"$project": {"_id": False, "DATA_HORA": True}},
            ]
        )

        return self["consumo_horario"].aggregate(pesquisa).next()["DATA_HORA"]

    def calcular_risco_modulacao_todo_periodo(self) -> pl.LazyFrame:
        """
        A cálculo do risco se dá para todo o período

        Returns
        -------
        pl.LazyFrame
            LazyFrame contedo o cálculo do risco para todo o período
        """
        return pl.LazyFrame(
            (
                doc
                for doc in self["consumo_horario"].aggregate(
                    [
                        # Operador match acrescentado somente para testes para agilizar a pesquisa
                        # {"$match": {"CODIGO_CARGA": {"$in": [70068, 899945]}}},
                        {"$project": {"_id": False}},
                        {
                            "$addFields": {
                                "mw_preco_baixo_2025": {
                                    "$multiply": [
                                        "$CONSUMO_CARGA_AJUSTADO",
                                        "$PLD_BAIXO_2025",
                                    ]
                                },
                                "mw_preco_medio_2025": {
                                    "$multiply": [
                                        "$CONSUMO_CARGA_AJUSTADO",
                                        "$PLD_MEDIO_2025",
                                    ]
                                },
                                "mw_preco_alto_2025": {
                                    "$multiply": [
                                        "$CONSUMO_CARGA_AJUSTADO",
                                        "$PLD_ALTO_2025",
                                    ]
                                },
                                "mw_preco_estresse_2025": {
                                    "$multiply": [
                                        "$CONSUMO_CARGA_AJUSTADO",
                                        "$PLD_ESTRESSE_2025",
                                    ]
                                },
                                "mw_preco_baixo_2026": {
                                    "$multiply": [
                                        "$CONSUMO_CARGA_AJUSTADO",
                                        "$PLD_BAIXO_2026",
                                    ]
                                },
                                "mw_preco_medio_2026": {
                                    "$multiply": [
                                        "$CONSUMO_CARGA_AJUSTADO",
                                        "$PLD_MEDIO_2026",
                                    ]
                                },
                                "mw_preco_alto_2026": {
                                    "$multiply": [
                                        "$CONSUMO_CARGA_AJUSTADO",
                                        "$PLD_ALTO_2026",
                                    ]
                                },
                                "mw_preco_estresse_2026": {
                                    "$multiply": [
                                        "$CONSUMO_CARGA_AJUSTADO",
                                        "$PLD_ESTRESSE_2026",
                                    ]
                                },
                            }
                        },
                        {
                            "$group": {
                                "_id": "$CODIGO_CARGA",
                                "soma_mw_preco_baixo_2025": {
                                    "$sum": "$mw_preco_baixo_2025"
                                },
                                "soma_mw_preco_medio_2025": {
                                    "$sum": "$mw_preco_medio_2025"
                                },
                                "soma_mw_preco_alto_2025": {
                                    "$sum": "$mw_preco_alto_2025"
                                },
                                "soma_mw_preco_estresse_2025": {
                                    "$sum": "$mw_preco_estresse_2025"
                                },
                                "soma_mw_preco_baixo_2026": {
                                    "$sum": "$mw_preco_baixo_2026"
                                },
                                "soma_mw_preco_medio_2026": {
                                    "$sum": "$mw_preco_medio_2026"
                                },
                                "soma_mw_preco_alto_2026": {
                                    "$sum": "$mw_preco_alto_2026"
                                },
                                "soma_mw_preco_estresse_2026": {
                                    "$sum": "$mw_preco_estresse_2026"
                                },
                                "media_pld_baixo_2025": {"$avg": "$PLD_BAIXO_2025"},
                                "media_pld_medio_2025": {"$avg": "$PLD_MEDIO_2025"},
                                "media_pld_alto_2025": {"$avg": "$PLD_ALTO_2025"},
                                "media_pld_estresse_2025": {
                                    "$avg": "$PLD_ESTRESSE_2025"
                                },
                                "media_pld_baixo_2026": {"$avg": "$PLD_BAIXO_2026"},
                                "media_pld_medio_2026": {"$avg": "$PLD_MEDIO_2026"},
                                "media_pld_alto_2026": {"$avg": "$PLD_ALTO_2026"},
                                "media_pld_estresse_2026": {
                                    "$avg": "$PLD_ESTRESSE_2026"
                                },
                                "SOMA_CONSUMO_CARGA_AJUSTADO": {
                                    "$sum": "$CONSUMO_CARGA_AJUSTADO"
                                },
                            }
                        },
                        {
                            "$project": {
                                "_id": False,
                                "CODIGO_CARGA": "$_id",
                                "soma_mw_preco_baixo_2025": True,
                                "soma_mw_preco_medio_2025": True,
                                "soma_mw_preco_alto_2025": True,
                                "soma_mw_preco_estresse_2025": True,
                                "soma_mw_preco_baixo_2026": True,
                                "soma_mw_preco_medio_2026": True,
                                "soma_mw_preco_alto_2026": True,
                                "soma_mw_preco_estresse_2026": True,
                                "media_pld_baixo_2025": True,
                                "media_pld_medio_2025": True,
                                "media_pld_alto_2025": True,
                                "media_pld_estresse_2025": True,
                                "media_pld_baixo_2026": True,
                                "media_pld_medio_2026": True,
                                "media_pld_alto_2026": True,
                                "media_pld_estresse_2026": True,
                                "SOMA_CONSUMO_CARGA_AJUSTADO": True,
                            }
                        },
                        {
                            "$project": {
                                "CODIGO_CARGA": True,
                                "RISCO_BAIXO_2025": {
                                    "$divide": [
                                        {
                                            "$subtract": [
                                                "$soma_mw_preco_baixo_2025",
                                                {
                                                    "$multiply": [
                                                        "$media_pld_baixo_2025",
                                                        "$SOMA_CONSUMO_CARGA_AJUSTADO",
                                                    ]
                                                },
                                            ]
                                        },
                                        "$SOMA_CONSUMO_CARGA_AJUSTADO",
                                    ]
                                },
                                "RISCO_MEDIO_2025": {
                                    "$divide": [
                                        {
                                            "$subtract": [
                                                "$soma_mw_preco_medio_2025",
                                                {
                                                    "$multiply": [
                                                        "$media_pld_medio_2025",
                                                        "$SOMA_CONSUMO_CARGA_AJUSTADO",
                                                    ]
                                                },
                                            ]
                                        },
                                        "$SOMA_CONSUMO_CARGA_AJUSTADO",
                                    ]
                                },
                                "RISCO_ALTO_2025": {
                                    "$divide": [
                                        {
                                            "$subtract": [
                                                "$soma_mw_preco_alto_2025",
                                                {
                                                    "$multiply": [
                                                        "$media_pld_alto_2025",
                                                        "$SOMA_CONSUMO_CARGA_AJUSTADO",
                                                    ]
                                                },
                                            ]
                                        },
                                        "$SOMA_CONSUMO_CARGA_AJUSTADO",
                                    ]
                                },
                                "RISCO_ESTRESSE_2025": {
                                    "$divide": [
                                        {
                                            "$subtract": [
                                                "$soma_mw_preco_estresse_2025",
                                                {
                                                    "$multiply": [
                                                        "$media_pld_estresse_2025",
                                                        "$SOMA_CONSUMO_CARGA_AJUSTADO",
                                                    ]
                                                },
                                            ]
                                        },
                                        "$SOMA_CONSUMO_CARGA_AJUSTADO",
                                    ]
                                },
                                "RISCO_BAIXO_2026": {
                                    "$divide": [
                                        {
                                            "$subtract": [
                                                "$soma_mw_preco_baixo_2026",
                                                {
                                                    "$multiply": [
                                                        "$media_pld_baixo_2026",
                                                        "$SOMA_CONSUMO_CARGA_AJUSTADO",
                                                    ]
                                                },
                                            ]
                                        },
                                        "$SOMA_CONSUMO_CARGA_AJUSTADO",
                                    ]
                                },
                                "RISCO_MEDIO_2026": {
                                    "$divide": [
                                        {
                                            "$subtract": [
                                                "$soma_mw_preco_medio_2026",
                                                {
                                                    "$multiply": [
                                                        "$media_pld_medio_2026",
                                                        "$SOMA_CONSUMO_CARGA_AJUSTADO",
                                                    ]
                                                },
                                            ]
                                        },
                                        "$SOMA_CONSUMO_CARGA_AJUSTADO",
                                    ]
                                },
                                "RISCO_ALTO_2026": {
                                    "$divide": [
                                        {
                                            "$subtract": [
                                                "$soma_mw_preco_alto_2026",
                                                {
                                                    "$multiply": [
                                                        "$media_pld_alto_2026",
                                                        "$SOMA_CONSUMO_CARGA_AJUSTADO",
                                                    ]
                                                },
                                            ]
                                        },
                                        "$SOMA_CONSUMO_CARGA_AJUSTADO",
                                    ]
                                },
                                "RISCO_ESTRESSE_2026": {
                                    "$divide": [
                                        {
                                            "$subtract": [
                                                "$soma_mw_preco_estresse_2026",
                                                {
                                                    "$multiply": [
                                                        "$media_pld_estresse_2026",
                                                        "$SOMA_CONSUMO_CARGA_AJUSTADO",
                                                    ]
                                                },
                                            ]
                                        },
                                        "$SOMA_CONSUMO_CARGA_AJUSTADO",
                                    ]
                                },
                            }
                        },
                        {
                            "$setWindowFields": {
                                "sortBy": {"RISCO_BAIXO_2025": 1},
                                "output": {"colocacao_risco": {"$rank": {}}},
                            }
                        },
                    ]
                )
            )
        )

    def calcular_risco_modulacao_media_diaria(self) -> pl.LazyFrame:
        """
        O cálculo do risco primeiro é realizado para cada dia e depois é realizado a média
        dos riscos diários

        Returns
        -------
        pl.LazyFrame
            LazyFrame contendo a mpedia dos riscos diários
        """

        return pl.LazyFrame(
            (
                doc
                for doc in self["consumo_horario"].aggregate(
                    [
                        # {"$match": {"CODIGO_CARGA": {"$in": [70068, 899945]}}},
                        {"$project": {"_id": False}},
                        {
                            "$addFields": {
                                "mw_preco_baixo_2025": {
                                    "$multiply": [
                                        "$CONSUMO_CARGA_AJUSTADO",
                                        "$PLD_BAIXO_2025",
                                    ]
                                },
                                "mw_preco_medio_2025": {
                                    "$multiply": [
                                        "$CONSUMO_CARGA_AJUSTADO",
                                        "$PLD_MEDIO_2025",
                                    ]
                                },
                                "mw_preco_alto_2025": {
                                    "$multiply": [
                                        "$CONSUMO_CARGA_AJUSTADO",
                                        "$PLD_ALTO_2025",
                                    ]
                                },
                                "mw_preco_estresse_2025": {
                                    "$multiply": [
                                        "$CONSUMO_CARGA_AJUSTADO",
                                        "$PLD_ESTRESSE_2025",
                                    ]
                                },
                                "mw_preco_baixo_2026": {
                                    "$multiply": [
                                        "$CONSUMO_CARGA_AJUSTADO",
                                        "$PLD_BAIXO_2026",
                                    ]
                                },
                                "mw_preco_medio_2026": {
                                    "$multiply": [
                                        "$CONSUMO_CARGA_AJUSTADO",
                                        "$PLD_MEDIO_2026",
                                    ]
                                },
                                "mw_preco_alto_2026": {
                                    "$multiply": [
                                        "$CONSUMO_CARGA_AJUSTADO",
                                        "$PLD_ALTO_2026",
                                    ]
                                },
                                "mw_preco_estresse_2026": {
                                    "$multiply": [
                                        "$CONSUMO_CARGA_AJUSTADO",
                                        "$PLD_ESTRESSE_2026",
                                    ]
                                },
                            }
                        },
                        {
                            "$group": {
                                "_id": {
                                    "CODIGO_CARGA": "$CODIGO_CARGA",
                                    "DIA": {
                                        "$dateToString": {
                                            "date": "$DATA_HORA",
                                            "format": "%Y-%m-%d",
                                        }
                                    },
                                },
                                "soma_mw_preco_baixo_2025": {
                                    "$sum": "$mw_preco_baixo_2025"
                                },
                                "soma_mw_preco_medio_2025": {
                                    "$sum": "$mw_preco_medio_2025"
                                },
                                "soma_mw_preco_alto_2025": {
                                    "$sum": "$mw_preco_alto_2025"
                                },
                                "soma_mw_preco_estresse_2025": {
                                    "$sum": "$mw_preco_estresse_2025"
                                },
                                "soma_mw_preco_baixo_2026": {
                                    "$sum": "$mw_preco_baixo_2026"
                                },
                                "soma_mw_preco_medio_2026": {
                                    "$sum": "$mw_preco_medio_2026"
                                },
                                "soma_mw_preco_alto_2026": {
                                    "$sum": "$mw_preco_alto_2026"
                                },
                                "soma_mw_preco_estresse_2026": {
                                    "$sum": "$mw_preco_estresse_2026"
                                },
                                "media_pld_baixo_2025": {"$avg": "$PLD_BAIXO_2025"},
                                "media_pld_medio_2025": {"$avg": "$PLD_MEDIO_2025"},
                                "media_pld_alto_2025": {"$avg": "$PLD_ALTO_2025"},
                                "media_pld_estresse_2025": {
                                    "$avg": "$PLD_ESTRESSE_2025"
                                },
                                "media_pld_baixo_2026": {"$avg": "$PLD_BAIXO_2026"},
                                "media_pld_medio_2026": {"$avg": "$PLD_MEDIO_2026"},
                                "media_pld_alto_2026": {"$avg": "$PLD_ALTO_2026"},
                                "media_pld_estresse_2026": {
                                    "$avg": "$PLD_ESTRESSE_2026"
                                },
                                "SOMA_CONSUMO_CARGA_AJUSTADO": {
                                    "$sum": "$CONSUMO_CARGA_AJUSTADO"
                                },
                            }
                        },
                        {
                            "$addFields": {
                                "TRUNC_SOMA_CONSUMO": {
                                    "$trunc": ["$SOMA_CONSUMO_CARGA_AJUSTADO", 5]
                                }
                            }
                        },
                        {"$match": {"TRUNC_SOMA_CONSUMO": {"$ne": 0}}},
                        {
                            "$project": {
                                "_id": False,
                                "CODIGO_CARGA": "$_id.CODIGO_CARGA",
                                "DIA": "$_id.DIA",
                                "soma_mw_preco_baixo_2025": True,
                                "soma_mw_preco_medio_2025": True,
                                "soma_mw_preco_alto_2025": True,
                                "soma_mw_preco_estresse_2025": True,
                                "soma_mw_preco_baixo_2026": True,
                                "soma_mw_preco_medio_2026": True,
                                "soma_mw_preco_alto_2026": True,
                                "soma_mw_preco_estresse_2026": True,
                                "media_pld_baixo_2025": True,
                                "media_pld_medio_2025": True,
                                "media_pld_alto_2025": True,
                                "media_pld_estresse_2025": True,
                                "media_pld_baixo_2026": True,
                                "media_pld_medio_2026": True,
                                "media_pld_alto_2026": True,
                                "media_pld_estresse_2026": True,
                                "SOMA_CONSUMO_CARGA_AJUSTADO": True,
                            }
                        },
                        {
                            "$project": {
                                "CODIGO_CARGA": True,
                                "DIA": True,
                                "RISCO_BAIXO_2025": {
                                    "$divide": [
                                        {
                                            "$subtract": [
                                                "$soma_mw_preco_baixo_2025",
                                                {
                                                    "$multiply": [
                                                        "$media_pld_baixo_2025",
                                                        "$SOMA_CONSUMO_CARGA_AJUSTADO",
                                                    ]
                                                },
                                            ]
                                        },
                                        "$SOMA_CONSUMO_CARGA_AJUSTADO",
                                    ]
                                },
                                "RISCO_MEDIO_2025": {
                                    "$divide": [
                                        {
                                            "$subtract": [
                                                "$soma_mw_preco_medio_2025",
                                                {
                                                    "$multiply": [
                                                        "$media_pld_medio_2025",
                                                        "$SOMA_CONSUMO_CARGA_AJUSTADO",
                                                    ]
                                                },
                                            ]
                                        },
                                        "$SOMA_CONSUMO_CARGA_AJUSTADO",
                                    ]
                                },
                                "RISCO_ALTO_2025": {
                                    "$divide": [
                                        {
                                            "$subtract": [
                                                "$soma_mw_preco_alto_2025",
                                                {
                                                    "$multiply": [
                                                        "$media_pld_alto_2025",
                                                        "$SOMA_CONSUMO_CARGA_AJUSTADO",
                                                    ]
                                                },
                                            ]
                                        },
                                        "$SOMA_CONSUMO_CARGA_AJUSTADO",
                                    ]
                                },
                                "RISCO_ESTRESSE_2025": {
                                    "$divide": [
                                        {
                                            "$subtract": [
                                                "$soma_mw_preco_estresse_2025",
                                                {
                                                    "$multiply": [
                                                        "$media_pld_estresse_2025",
                                                        "$SOMA_CONSUMO_CARGA_AJUSTADO",
                                                    ]
                                                },
                                            ]
                                        },
                                        "$SOMA_CONSUMO_CARGA_AJUSTADO",
                                    ]
                                },
                                "RISCO_BAIXO_2026": {
                                    "$divide": [
                                        {
                                            "$subtract": [
                                                "$soma_mw_preco_baixo_2026",
                                                {
                                                    "$multiply": [
                                                        "$media_pld_baixo_2026",
                                                        "$SOMA_CONSUMO_CARGA_AJUSTADO",
                                                    ]
                                                },
                                            ]
                                        },
                                        "$SOMA_CONSUMO_CARGA_AJUSTADO",
                                    ]
                                },
                                "RISCO_MEDIO_2026": {
                                    "$divide": [
                                        {
                                            "$subtract": [
                                                "$soma_mw_preco_medio_2026",
                                                {
                                                    "$multiply": [
                                                        "$media_pld_medio_2026",
                                                        "$SOMA_CONSUMO_CARGA_AJUSTADO",
                                                    ]
                                                },
                                            ]
                                        },
                                        "$SOMA_CONSUMO_CARGA_AJUSTADO",
                                    ]
                                },
                                "RISCO_ALTO_2026": {
                                    "$divide": [
                                        {
                                            "$subtract": [
                                                "$soma_mw_preco_alto_2026",
                                                {
                                                    "$multiply": [
                                                        "$media_pld_alto_2026",
                                                        "$SOMA_CONSUMO_CARGA_AJUSTADO",
                                                    ]
                                                },
                                            ]
                                        },
                                        "$SOMA_CONSUMO_CARGA_AJUSTADO",
                                    ]
                                },
                                "RISCO_ESTRESSE_2026": {
                                    "$divide": [
                                        {
                                            "$subtract": [
                                                "$soma_mw_preco_estresse_2026",
                                                {
                                                    "$multiply": [
                                                        "$media_pld_estresse_2026",
                                                        "$SOMA_CONSUMO_CARGA_AJUSTADO",
                                                    ]
                                                },
                                            ]
                                        },
                                        "$SOMA_CONSUMO_CARGA_AJUSTADO",
                                    ]
                                },
                            }
                        },
                        {"$match": {"RISCO_BAIXO_2025": {"$ne": 0}}},
                        {
                            "$group": {
                                "_id": "$CODIGO_CARGA",
                                "RISCO_BAIXO_2025": {"$avg": "$RISCO_BAIXO_2025"},
                                "RISCO_MEDIO_2025": {"$avg": "$RISCO_MEDIO_2025"},
                                "RISCO_ALTO_2025": {"$avg": "$RISCO_ALTO_2025"},
                                "RISCO_ESTRESSE_2025": {"$avg": "$RISCO_ESTRESSE_2025"},
                                "RISCO_BAIXO_2026": {"$avg": "$RISCO_BAIXO_2026"},
                                "RISCO_MEDIO_2026": {"$avg": "$RISCO_MEDIO_2026"},
                                "RISCO_ALTO_2026": {"$avg": "$RISCO_ALTO_2026"},
                                "RISCO_ESTRESSE_2026": {"$avg": "$RISCO_ESTRESSE_2026"},
                            }
                        },
                        {
                            "$project": {
                                "_id": False,
                                "CODIGO_CARGA": "$_id",
                                "RISCO_BAIXO_2025": True,
                                "RISCO_MEDIO_2025": True,
                                "RISCO_ALTO_2025": True,
                                "RISCO_ESTRESSE_2025": True,
                                "RISCO_BAIXO_2026": True,
                                "RISCO_MEDIO_2026": True,
                                "RISCO_ALTO_2026": True,
                                "RISCO_ESTRESSE_2026": True,
                            }
                        },
                        {
                            "$setWindowFields": {
                                "sortBy": {"RISCO_BAIXO_2025": 1},
                                "output": {"colocacao_risco": {"$rank": {}}},
                            }
                        },
                    ]
                )
            )
        )

    def get_data_consumo_horario(self, menor: bool = True) -> datetime:
        """
        Obtem a prrimeira ou a última data dos registros da tabela

        Parameters
        ----------
        menor : bool, optional
            Obter a menor caso true e maior caso false, by default True

        Returns
        -------
        datetime
            Menor ou maior data dos registros da tabela
        """

        return (
            self["consumo_horario"]
            .aggregate(
                [
                    {
                        "$group": {
                            "_id": None,
                            "data": {"$min" if menor else "$max": "$DATA_HORA"},
                        }
                    }
                ]
            )
            .next()["data"]
        )
