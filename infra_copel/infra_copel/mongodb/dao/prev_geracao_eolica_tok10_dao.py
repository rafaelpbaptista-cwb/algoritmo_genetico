"""Módulo que possui o objeto do tipo DAO (Data Access Object) para a collection prev_geracao_eolica_tok10"""
import pandas as pd
from infra_copel.mongodb.tempook import MongoTempoOK


class PrevGeracaoEolicaTok10DAO:
    def __init__(self) -> None:
        self.prev_geracao_eolica_tok10 = MongoTempoOK()["prev_geracao_eolica_tok10"]

    def buscar_prev_geracao_eolica_horario_tok10_mais_recente(self) -> pd.DataFrame:
        """
        Busca a útima previsão de MWm horário para todo o Brasil

        Returns
        -------
        pd.DataFrame
            pd.DataFrame
        """

        return pd.DataFrame(
            list(
                self.prev_geracao_eolica_tok10.aggregate(
                    [
                        {
                            "$project": {
                                "_id": False,
                                "periodo": True,
                                "data_previsao": True,
                                "ger_eol_mwm": {"$add": ["$N", "$NE", "$S"]},
                            }
                        },
                        {
                            "$group": {
                                "_id": {
                                    "data_previsao": "$data_previsao",
                                },
                                "periodo": {"$max": "$periodo"},
                                "ger_eol_mwm": {"$max": "$ger_eol_mwm"},
                            }
                        },
                        {
                            "$project": {
                                "_id": False,
                                "data_previsao": "$_id.data_previsao",
                                "ger_eol_mwm": "$ger_eol_mwm",
                                "periodo": "$periodo",
                            }
                        },
                    ]
                )
            )
        )
