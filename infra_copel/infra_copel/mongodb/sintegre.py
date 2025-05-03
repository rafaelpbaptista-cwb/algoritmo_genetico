"""Conexão com a database sintegre."""

from infra_copel.mongodb.base import MongoDatabase


class MongoSintegre(MongoDatabase):
    """
    Classe que representa conexão com o 'sintegre' no MongoDB.

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
        super().__init__('sintegre')
