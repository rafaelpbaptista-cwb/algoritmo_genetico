# -*- coding: utf-8 -*-
"""Conexão com a database refinitiv."""

from infra_copel.mongodb.base import MongoDatabase


class MongoRefinitiv(MongoDatabase):
    """
    Classe que representa conexão com o 'refinitiv' no MongoDB.

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
        super().__init__('refinitiv')
