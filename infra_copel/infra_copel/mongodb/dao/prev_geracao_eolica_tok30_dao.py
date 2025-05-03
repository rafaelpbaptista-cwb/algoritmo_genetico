"""Módulo que possui o objeto do tipo DAO (Data Access Object) para a collection prev_geracao_eolica_tok30"""
from datetime import datetime, timedelta
import pandas as pd
from infra_copel.mongodb.tempook import MongoTempoOK


class PrevGeracaoEolicaTok30DAO:
    def __init__(self) -> None:
        self.prev_geracao_eolica_tok30 = MongoTempoOK().prev_geracao_eolica_tok30

    def buscar_prev_geracao_eolica_horario_tok30_mais_recente(
        self, data_previsao: datetime
    ) -> pd.DataFrame:
        """
        Busca a útima previsão de MWm horário para todo o Brasil

        Returns
        -------
        pd.DataFrame
            pd.DataFrame
        """

        df = self.prev_geracao_eolica_tok30[
            (
                self.prev_geracao_eolica_tok30["periodo"]
                == self.prev_geracao_eolica_tok30["periodo"].max()
            )
            & (
                self.prev_geracao_eolica_tok30["data_previsao"]
                >= (data_previsao + timedelta(hours=1))
            )
        ]

        return (
            df.set_index(["data_previsao", "periodo"])
            .sum(axis="columns")
            .reset_index()
            .rename(columns={0: "ger_eol_mwm"})
        )
 