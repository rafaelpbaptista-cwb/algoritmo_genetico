"""Módulo contendo todas as classes de acesso aos dados. Esse tipo de classe centraliza todas as buscas realizadas nas collections do MongoDB"""
from infra_copel.mongodb.dao.pld_dao import PldDAO
from infra_copel.mongodb.dao.hist_temp_ponderado_dao import HistTempPonderadoDAO
from infra_copel.mongodb.dao.prev_temp_ponderado_tok10_dao import (
    PrevTempPonderadoTok10DAO,
)
from infra_copel.mongodb.dao.prev_temp_ponderado_tok30_dao import (
    PrevTempPonderadoTok30DAO,
)
from infra_copel.mongodb.dao.prev_geracao_eolica_ons_dao import PrevGeracaoEolicaONSDAO
from infra_copel.mongodb.dao.prev_geracao_eolica_tok10_dao import (
    PrevGeracaoEolicaTok10DAO,
)
from infra_copel.mongodb.dao.prev_geracao_eolica_tok30_dao import (
    PrevGeracaoEolicaTok30DAO,
)
from infra_copel.mongodb.dao.ons_geracao_usinas_dao import GeracaoUsinasONSDAO

from infra_copel.mongodb.dao.prev_geracao_eolica_tokia_dao import PrevGeracaoEolicaTokIaDAO