""" Arquivo contendo métodos de utilidade a ser usada em diversas partes do módulo """

import sys
from datetime import datetime

def somar_qtdade_energia_cromossomos(lista_cromossomo: list) -> int:
    """
    Realiza a soma da quantidade de energia vendida setada em cada cromossomo do indivíduo

    Args:
        lista_cromossomo (list): lista de cromossomos do indivíduo. Lista entidade GeneRepresentacaoMes

    Returns:
        int: _description_
    """
    
    return sum([gene.qtdade_energia_mwm_venda for gene in lista_cromossomo])

def get_qtdade_horas_ano():
    primeiro_dia_ano = datetime(year=datetime.now().year, month=1, day=1)
    ultimo_dia_ano = datetime(year=datetime.now().year, month=12, day=31)

    return ((ultimo_dia_ano - primeiro_dia_ano).days + 1) * 24

def get_maior_nota_avaliacao_disponivel(tamanho_populacao: int):
    return round(sys.maxsize / tamanho_populacao)