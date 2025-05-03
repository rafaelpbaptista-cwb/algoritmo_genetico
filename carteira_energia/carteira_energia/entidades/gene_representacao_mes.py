from dataclasses import dataclass

@dataclass
class GeneRepresentacaoMes():
    """
    Entidade que representa um gene do cromossomo do indivíduo.
    Cada cromossomo do indivíduo representa um mês do estudo. Propriedade mês representa a ordem sequencial do mês (1º, 2º, 3º, etc mês do estudo)
    """
    sequencia_mes: int
    preco_pld: int
    risco: int
    qtdade_energia_mwm_venda: int = 0