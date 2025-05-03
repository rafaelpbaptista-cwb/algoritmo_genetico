"""Módulo que possui o objeto do tipo DAO (Data Access Object) para a collection prev_geracao_eolica_tokia"""
from datetime import datetime, timedelta
import pandas as pd
from infra_copel.mongodb.tempook import MongoTempoOK


class PrevGeracaoEolicaTokIaDAO:
    def __init__(self) -> None:
        self.prev_geracao_eolica_tokia = MongoTempoOK().prev_geracao_eolica_tok_ia

    def buscar_prev_geracao_eolica_horario_tokia_mais_recente(
        self, data_previsao: datetime
    ) -> pd.DataFrame:
        """
        Busca a útima previsão de MWm horário para todo o Brasil, com os dados de Tok Ia
        (os campos data previsao e periodo nessa collection são os inversos dos da tok30)

        Returns
        -------
        pd.DataFrame
            pd.DataFrame
        """

        df = self.prev_geracao_eolica_tokia[
            (
                self.prev_geracao_eolica_tokia["data_previsao"]
                == self.prev_geracao_eolica_tokia["data_previsao"].max()
            )
            & (
                self.prev_geracao_eolica_tokia["periodo"]
                >= (data_previsao + timedelta(hours=1))
            )
        ]

        return (
            df.set_index(["data_previsao", "periodo"])
            .sum(axis="columns")
            .reset_index()
            .rename(columns={0: "ger_eol_mwm"})
        )
 