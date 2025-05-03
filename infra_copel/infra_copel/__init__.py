# -*- coding: utf-8 -*-
"""
Biblioteca com infraestrutura de conexões.

Para conexão com APIs, FTP, MongoDB, SharePoint, etc.
"""
# MongoDB
from infra_copel.mongodb.base import MongoDatabase

from infra_copel.mongodb.dcide import MongoDcide
from infra_copel.mongodb.historico_oficial import MongoHistoricoOficial
from infra_copel.mongodb.perfil_consumo import PerfilConsumo
from infra_copel.mongodb.tempook import MongoTempoOK
from infra_copel.mongodb.climatempo import MongoClimaTempo
from infra_copel.mongodb.refinitiv import MongoRefinitiv
from infra_copel.mongodb.rodadas import MongoRodadas
from infra_copel.mongodb.bbce import MongoBBCE


from infra_copel.mongodb.util import df_to_docs

# Sharepoint
from infra_copel.sharepoint.site_copel import SharepointSiteCopel

# FTP
from infra_copel.ftp.base import FTPBase

# Versioneer
from . import _version
__version__ = _version.get_versions()['version']
