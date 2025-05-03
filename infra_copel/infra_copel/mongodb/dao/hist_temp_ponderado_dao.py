"""Classe DAO (Data Access Objeto) para a collection hist_temp_ponderado"""

from datetime import datetime
from infra_copel.mongodb.tempook import MongoTempoOK


class HistTempPonderadoDAO:
    """
    Data Acccess Object para a collection hist_temp_ponderado
    """    
    def __init__(self) -> None:
        self.hist_temp_ponderado = MongoTempoOK()["hist_temp_ponderado"]

    def buscar_ultimo_periodo_historico(self) -> datetime:
        """
        Obtém a última data do histórico

        Returns
        -------
        datetime
            Última data do histórico
        """

        return self.hist_temp_ponderado.aggregate(
            [{"$group": {"_id": None, "data": {"$max": "$periodo"}}}]
        ).next()["data"]
