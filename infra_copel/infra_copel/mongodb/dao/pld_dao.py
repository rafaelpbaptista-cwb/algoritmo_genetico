"""
Data Access Object (DAO)
Módulo responsável em fazer acesso aos dados de PLD
"""
import re
import pandas as pd
from infra_copel.mongodb.historico_oficial import MongoHistoricoOficial


class PldDAO:
    def __init__(self) -> None:
        self.pld_horario = MongoHistoricoOficial()["pld_horario"]

    def buscar_pld_descolamento_piso(self, ano_pesquisa: int) -> pd.DataFrame:
        """
        Obtêm para um determinado ano os dias cujo PLD descolou do piso

        Parameters
        ----------
        preco_piso : float
            Preço piso PLD
        ano_pesquisa : int
            Ano que os dados serão obtidos

        Returns
        -------
        pd.DataFrame
            Datas que o PLD descolou do piso
        """
        preco_piso = MongoHistoricoOficial().df_limites_pld_tarifas.iloc[-1]["PLD_MIN"]

        df = pd.DataFrame(
            list(
                self.pld_horario.aggregate(
                    [
                        {
                            "$group": {
                                "_id": {
                                    "$dateToString": {
                                        "date": "$hora",
                                        "format": "%Y-%m-%d",
                                    }
                                },
                                "pld_medio": {"$avg": "$SE"},
                            }
                        },
                        {
                            "$match": {
                                "_id": re.compile(str(ano_pesquisa)),
                                "pld_medio": {"$gt": preco_piso},
                            }
                        },
                    ]
                )
            )
        )

        if df.empty:
            return pd.DataFrame({"data": [], "pld_medio": []})
        else:
            df.rename(columns={"_id": "data"}, inplace=True)
            df["data"] = pd.to_datetime(df["data"])

            return df.sort_values("data")
