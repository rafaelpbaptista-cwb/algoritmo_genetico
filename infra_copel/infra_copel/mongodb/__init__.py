# -*- coding: utf-8 -*-
"""Para facilitar conexão com o MongoDB."""
from infra_copel.mongodb.base import MongoDatabase

from infra_copel.mongodb.dcide import MongoDcide
from infra_copel.mongodb.historico_oficial import MongoHistoricoOficial
from infra_copel.mongodb.sintegre import MongoSintegre
from infra_copel.mongodb.refinitiv import MongoRefinitiv
from infra_copel.mongodb.rodadas import MongoRodadas
from infra_copel.mongodb.prospec import MongoProspec

from infra_copel.mongodb.util import df_to_docs
