# -*- coding: utf-8 -*-
"""Conexão com a database rodadas."""
import pandas as pd
import pymongo
from datetime import datetime
from infra_copel.mongodb.base import MongoDatabase
from pymongo.command_cursor import CommandCursor


class MongoRodadas(MongoDatabase):
    """
    Classe que representa conexão com 'rodadas' no MongoDB.

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
        super().__init__("rodadas")

    @property
    def df_deck_entrada_prev_carga_dessem(self) -> pd.DataFrame:
        """
        Collection contendo os dados do deck de entrada para que o
        modelos de previsão carga DESSEM possa fazer a sua previsão

        Returns
        -------
        pd.DataFrame
            DataFrame
        """
        return self.collection_to_df(collection="deck_entrada_prev_carga_dessem")

    @property
    def df_prev_carga_dessem(self):
        """
        Informações de previsão de carga a ser utilizado no modelos DESSEM.
        Descrição dos campos:
        - submercado: submercado da previsão
        - periodo: data que a previsão foi realizada
        - carga_mw: carga em megawat. Valor dado de 30 em 30 min
        - data_previsao: a data da previsão de carga (data, hora e minuto)

        Segue um exemplo de interpretação dos dados: na data de 08/12/2023 (período) foi realizado a previsão de que no dia 09/12/2023 às 10h (data_previsao) a carga será de 30MWm.
        """
        return self.collection_to_df(collection="prev_carga_dessem")

    def resumo_mensal_dc_sem_previsao_dcide(
        self, submercado: str = "SIN"
    ) -> CommandCursor:
        """
        Obtem todas as informações da rodada rolling que ainda não possui informações de previsão dcide

        Parameters
        submercado : str
            Submercado a ser filtrado. Default, SIN
        Returns
        -------
        pd.DataFrame
            Cursor para navegar nas rodadas que ainda não possui informações de previsão dcide
        """

        return self["resumo_mensal_dc"].aggregate(
            [
                {
                    "$match": {
                        "valor_dcide": {"$exists": False},
                        "submercado": submercado,
                    }
                },
                {"$group": {"_id": "$serie", "periodo": {"$min": "$periodo"}}},
                {"$sort": {"periodo": 1}},
            ]
        )

    def resumo_mensal_dc(self) -> pd.DataFrame:
        """
        Dados mensais dos encadeados.
        """
        df = self.collection_to_df("resumo_mensal_dc")
        df["periodo"] = pd.PeriodIndex(df["periodo"], freq="M")
        df.set_index(
            [
                "serie",
                "prefixo",
                "data_rodada",
                "previsao",
                "friccao",
                "sufixo",
                "submercado",
                "periodo",
            ],
            inplace=True,
        )

        return df.drop(columns="valor_dcide")

    def inserir_previsao_dcide_resumo_mensal_dc(
        self, serie: str, periodo: pd.Timestamp, valor: dict
    ) -> None:
        """
        Atualiza os dados da collection resumo_mensal_dc para para inserir informações de previsão dcide

        Parameters
        ----------
        serie : str
            Serie a ser atualizada
        periodo : pd.Timestamp
            Data da serie a ser atualizada
        valor : dict
            Valores das previsões dcide
        """

        filtro = {"serie": serie, "periodo": periodo}
        atualizacao = {"$push": {"valor_dcide": valor}}

        self["resumo_mensal_dc"].update_many(filtro, atualizacao)

    def buscar_previsao_dcide(
        self, rolling_str: str = "", realizar_pivot: bool = True
    ) -> pd.DataFrame:
        """
        Retorna as previsões dcide de todos os estudos relacionados a tag rolling passada para parâmetro.

        Parameters
        ----------
        rolling_str : str
            Tag rolling. Se não informado, será obtido todas as previsões dcide. Default is ''

        realizar_pivot : bool
            As colunas do dataframe serão os meses do ano. Default is True

        Returns
        -------
        pd.DataFrame
            DataFrame contendo as previsões dcide dos estudos relacionados a tag rolling
        """

        resultado = self["resumo_mensal_dc"].aggregate(
            [
                {"$match": {"serie": {"$regex": rolling_str}}},
                {"$unwind": "$valor_dcide"},
                {
                    "$project": {
                        "_id": False,
                        "serie": True,
                        "periodo": True,
                        "modelo": "$valor_dcide.modelo",
                        "valor_dcide_a1": "$valor_dcide.valor_a1",
                        "is_selecao_previsao": "$valor_dcide.is_selecao_previsores",
                    }
                },
            ]
        )

        df = pd.DataFrame(list(resultado))

        if not df.empty:
            if realizar_pivot:
                df = df.pivot_table(
                    index=["serie", "modelo", "is_selecao_previsao"],
                    columns="periodo",
                    values="valor_dcide_a1",
                )
                df.columns = pd.PeriodIndex(df.columns, freq="M")
            else:
                df["periodo"] = df["periodo"].dt.to_period("M")

        return df

    def get_primeiro_periodo_resumo_mensal_dc(self) -> datetime:
        """
        Obtem a data mais antiga (primeira data) da collection resumo_mensal_dc.

        Returns
        -------
        datetime
            data mais antiga do registro da collection resumo_mensal_dc
        """

        return (
            self["resumo_mensal_dc"]
            .find()
            .sort("periodo", pymongo.ASCENDING)
            .limit(1)
            .next()["periodo"]
        )

    def apagar_informacoes_previsao_dcide_resumo_mensal_dc(self, serie: str) -> None:
        self["resumo_mensal_dc"].update_many(
            {"serie": serie}, {"$unset": {"valor_dcide": ""}}
        )

    def get_ultima_serie_resumo_mensal_dc(self) -> pd.DataFrame:
        """
        Obtem a última série temporal da collection resumo_mensal_dc que possui mais meses com previsão decomp.

        Returns
        -------
        pd.DataFrame
            Série rolling mais recente persistida na base de dados.
        """
        resultado = self["resumo_mensal_dc"].aggregate(
            [
                {
                    "$group": {
                        "_id": {"prefixo": "$prefixo", "data_rodada": "$data_rodada"},
                        "qtdade": {"$sum": 1},
                        "periodo": {"$min": "$periodo"},
                    }
                },
                {"$sort": {"periodo": -1, "qtdade": -1}},
                {"$limit": 1},
                {
                    "$project": {
                        "_id": False,
                        "periodo": True,
                        "qtdade": True,
                        "serie": {"$concat": ["$_id.prefixo", "-", "$_id.data_rodada"]},
                    }
                },
            ]
        )

        return pd.DataFrame(list(resultado))

    def get_estudos_duplicidade_tags(
        self, nome_collection: str = "decomp"
    ) -> pd.DataFrame:
        """
        Obtem os estudos que possuem a mesma tag rolling

        Returns
        -------
        pd.DataFrame
            Estudos com a mesma tag rolling
        """
        return pd.DataFrame(
            list(
                self[nome_collection].aggregate(
                    [
                        {
                            "$group": {
                                "_id": "$api_status.tag",
                                "qtdade": {"$count": {}},
                            }
                        },
                        {"$match": {"qtdade": {"$gt": 1}}},
                    ]
                )
            )
        )
