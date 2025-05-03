"""Conexão com a database prospec."""

import warnings

from infra_copel.mongodb.historico_oficial import MongoHistoricoOficial
from infra_copel.mongodb.util import df_to_docs, extend_period_seasonal

# Ignora warnings de resampling
# https://github.com/pandas-dev/pandas/issues/57033
warnings.filterwarnings(
    "ignore",
    message="Resampling with a PeriodIndex is deprecated."
    " Cast index to DatetimeIndex before resampling instead.",
)
warnings.filterwarnings(
    "ignore",
    message="The 'convention' keyword in DataFrame.resample is deprecated"
    " and will be removed in a future version."
    " Explicitly cast PeriodIndex to DatetimeIndex before resampling instead.",
)

import pandas as pd

from infra_copel.mongodb.base import MongoDatabase
from infra_copel.mongodb.prospec_aux import periodo_nome_deck


class MongoProspec(MongoDatabase):

    def __init__(self):
        """
        Construtor da classe.

        Cria o objeto baseado no nome da database.
        """
        super().__init__("prospec")

    def salvar_compila(
        self, estudo_titulo: str, estudo_id: int, dado: str, dataframe: pd.DataFrame
    ):
        """
        Salva algum arquivo de compila_{dado}.csv no banco.

        Parameters
        ----------
        estudo_titulo : str
            Título do estudo no Prospec.
        estudo_id : int
            Id do estudo no Prospec
        dado : str
            Dado a ser salvo.
            Geralmente a string presente no nome do arquivo compila_{dado}.csv
        dataframe : DataFrame
            Dataframe com os dados, obtido na biblioteca do prospec

        """
        df = dataframe.copy()
        df["estudo_titulo"] = estudo_titulo
        df["estudo_id"] = estudo_id
        df["dado"] = dado

        # Só para deixar estas colunas como iniciais
        df = df.set_index(["estudo_titulo", "estudo_id", "dado", "deck"]).reset_index()

        docs = df.to_dict("records")

        # Para garantir que não haja dados duplicados
        self["compila"].delete_many({"estudo_id": estudo_id, "dado": dado})
        self["compila"].insert_many(docs)

    def ler_compila(self, encadeado: str, dado: str) -> pd.DataFrame:

        df = self.collection_to_df(
            "compila",
            query={
                "estudo_titulo": {"$regex": encadeado},
                "sensibilidade": "Original",
                "dado": dado,
                "semanal": True,
            },
            projection={
                "_id": False,
                "estudo_id": False,
                "sensibilidade": False,
                "dado": False,
                "semanal": False,
            },
        )

        df["periodo"] = df["deck"].apply(periodo_nome_deck)

        return df

    def df_cmo_mensal(self, encadeado: str) -> pd.DataFrame:

        df = self.ler_compila(encadeado, "cmo_medio")
        df = df.set_index("periodo").sort_index()
        df.drop_duplicates(inplace=True)

        lista_df_agg = []
        for grupo, df_g in df.groupby("estudo_titulo"):
            df_g = df_g.resample("D").ffill()
            df_g = df_g.resample("M").mean(numeric_only=True)
            df_g["estudo_titulo"] = grupo
            lista_df_agg.append(df_g)

        new_df = pd.concat(lista_df_agg)
        new_df = self._filtrar_agr_dados(df, new_df)

        new_df = new_df.set_index(["estudo_titulo", "periodo"]).sort_index()
        new_df.columns.name = "submercado"

        return new_df

    def _filtrar_agr_dados(self, df_ler_compila: pd.DataFrame, new_df: pd.DataFrame):
        """
        Filtro para não mostrar meses anteriores ao estudo
        Isso pode ocorrer quando a semana operativa começa
        num mês e finalizao em outro. Ex: Semana 2024-12-28/2025-01-03
        começa em dezembro e finaliza em janeiro. Filtro para não
        mostrar dados de 2024-12

        Parameters
        ----------
        df_ler_compila : pd.DataFrame
            DataFrame retornado pelo método self.ler_compila
        new_df : pd.DataFrame
            DataFrame que está sendo trabalhado e que será retornado pelo
            método
        """
        new_df = new_df.reset_index()
        return new_df[
            new_df["periodo"] >= df_ler_compila.index[0].end_time.to_period("M")
        ]

    def df_ena_mensal(self, encadeado: str) -> pd.DataFrame:

        df = self.ler_compila(encadeado, "ena")
        df = df.set_index("periodo").sort_index()
        df.drop_duplicates(inplace=True)

        lista_df_agg = []
        for grupo, df_g in df.groupby("estudo_titulo"):
            df_g = df_g.resample("D").ffill()
            df_g = df_g.resample("M").mean(numeric_only=True)
            df_g["estudo_titulo"] = grupo
            lista_df_agg.append(df_g)

        new_df = pd.concat(lista_df_agg)
        new_df = self._filtrar_agr_dados(df, new_df)

        new_df = new_df.set_index(["estudo_titulo", "periodo"]).sort_index()
        new_df.columns.name = "submercado"

        return new_df

    def df_ear_mensal(self, encadeado: str) -> pd.DataFrame:

        df = self.ler_compila(encadeado, "ea")
        df = df.set_index("periodo").sort_index()
        df.drop_duplicates(inplace=True)

        lista_df_agg = []
        for grupo, df_g in df.groupby("estudo_titulo"):
            df_g = df_g.resample("D", convention="end").interpolate(limit_area="inside")
            df_g = df_g.resample("M").last(numeric_only=True)
            df_g["estudo_titulo"] = grupo
            lista_df_agg.append(df_g)
        new_df = pd.concat(lista_df_agg)
        new_df = (
            new_df.reset_index().set_index(["estudo_titulo", "periodo"]).sort_index()
        )
        new_df.columns.name = "submercado"

        return new_df

    def df_ena_pct_mensal(self, encadeado: str) -> pd.DataFrame:

        df_ena = self.df_ena_mensal(encadeado)

        df_mlt = MongoHistoricoOficial().df_ena_mlt

        df_mlt_ext = extend_period_seasonal(df_mlt, 24)

        map_df_ena_pct = {}
        for enc_name, ena in df_ena.groupby(level="estudo_titulo"):
            df_mwm = ena.loc[enc_name]
            df_pct = (df_mwm / df_mlt_ext).reindex(df_mwm.index).round(3) * 100
            map_df_ena_pct[enc_name] = df_pct
        df_ena_pct = pd.concat(map_df_ena_pct, names=["estudo_titulo"])

        return df_ena_pct

    def update_compilado_mensal(self, encadeado: str):

        dados_df = {}
        dados_df["CMO"] = self.df_cmo_mensal(encadeado)
        dados_df["EAR"] = self.df_ear_mensal(encadeado)
        dados_df["ENA"] = self.df_ena_mensal(encadeado)
        # TODO: ver a separação deste dado para ser calculado de acordo
        # com a MLT atualizada
        dados_df["ENA_PCT"] = self.df_ena_pct_mensal(encadeado)

        dados_ser = []
        for dado, df in dados_df.items():
            ser_dado = df.stack()
            ser_dado.name = dado
            dados_ser.append(ser_dado)

        df_all = pd.concat(dados_ser, axis=1).reset_index()
        df_all[["prefixo", "tipo_prazo", "data_rodada", "previsao"]] = df_all[
            "estudo_titulo"
        ].str.split("-", expand=True)

        docs = df_to_docs(df_all)

        if docs:
            self["compilado_mensal"].delete_many(
                {"estudo_titulo": {"$regex": encadeado}}
            )
            self["compilado_mensal"].insert_many(docs)
        else:
            print(f"Encadeado {encadeado} sem dados")

    def df_compilado_mensal(self, encadeado: str) -> pd.DataFrame:

        df = self.collection_to_df(
            "compilado_mensal", {"estudo_titulo": {"$regex": encadeado}}
        )

        df["periodo"] = pd.PeriodIndex(df["periodo"], freq="M")

        return df
