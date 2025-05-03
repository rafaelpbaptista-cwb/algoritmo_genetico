"""Módulo que possui o objeto do tipo DAO (Data Access Object) para a collection ons_geracao_usinas"""

import pandas as pd
from datetime import datetime
from infra_copel.mongodb.historico_oficial import MongoHistoricoOficial


class GeracaoUsinasONSDAO:
    """
    Dados relacionados ao histórico consolidado da geração das usinas gerenciadas pelo ONS
    """

    def __init__(self) -> None:
        self.ons_geracao_usinas = MongoHistoricoOficial()["ons_geracao_usinas"]

    def buscar_geracao_eolica_horario(self) -> pd.DataFrame:
        """
        Busca a qtdade de MWm horário para todo o Brasil

        Returns
        -------
        pd.DataFrame
            DataFrame com a geração horária das usinas eólicas
        """
        return pd.DataFrame(
            list(
                self.ons_geracao_usinas.aggregate(
                    [
                        {"$match": {"tipo_combustivel": "Eólica"}},
                        {
                            "$group": {
                                "_id": "$periodo",
                                "ger_eol_mwm": {"$sum": "$geracao_mwm"},
                            }
                        },
                    ]
                )
            )
        )

    def buscar_ultimo_periodo_historico(self) -> datetime:
        """
        Obtém a última data do histórico

        Returns
        -------
        datetime
            Última data do histórico
        """

        return self.ons_geracao_usinas.aggregate(
            [{"$group": {"_id": None, "data": {"$max": "$periodo"}}}]
        ).next()["data"]
