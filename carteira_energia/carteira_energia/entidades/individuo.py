import random
#import sys
# import import_ipynb
from carteira_energia.formulas.formulas_agoritmo_genetico import calcular_variavel_dsva, calcular_variavel_rppda, calcular_funcao_minimizar_riscos_compra_energia
from dataclasses import dataclass, field
from carteira_energia.entidades.gene_representacao_mes import GeneRepresentacaoMes
from carteira_energia.util.utilidades import somar_qtdade_energia_cromossomos, get_qtdade_horas_ano, get_maior_nota_avaliacao_disponivel
from carteira_energia.entidades.configuracao_cenario import ConfiguracaoCenario

@dataclass
class Individuo:
    """
    Classe representando o indivíduo do algoritmo genético
    O indivíduo representa uma possível resposta para o nosso problema.
    
    A propriedade *lista_cromossomo* é uma lista de 12 elementos onde cada indice representa a sugestão de compra ou venda de energia para um determinado mês
    """

    configuracao_cenario: ConfiguracaoCenario = field(repr=False)
    _estourou_limite_venda_energia: bool = field(default=False, repr=False)
    nota_maxima_individuo_rodada: int = field(default=0, repr=False)
    geracao: int = field(default=0, repr=False)
    lista_cromossomo: list[GeneRepresentacaoMes] = field(default_factory=list)
    gerar_cromossomo: bool = field(default=True, repr=False)

    def __post_init__(self):
        self._gerar_cromossomo_aleatorio()

    def _gerar_cromossomo_aleatorio(self):
        qtdade_meses_ano = len(self.configuracao_cenario.lista_preco_pld_mes)

        if self.gerar_cromossomo:
            for mes in range(qtdade_meses_ano):
                qtdade_energia_mwm_vendida_mes = round(random.random() * self.configuracao_cenario.meta_anual_venda_kwm)
                
                gene = GeneRepresentacaoMes(sequencia_mes=mes,
                                            preco_pld=self.configuracao_cenario.lista_preco_pld_mes[mes],
                                            risco=self.configuracao_cenario.lista_risco_mes[mes],
                                            qtdade_energia_mwm_venda=qtdade_energia_mwm_vendida_mes)

                self.lista_cromossomo.append(gene)

    @property
    def nota_avaliacao(self):
        self.verificar_estouro_limite_qtdade_energia()
        
        if not self._estourou_limite_venda_energia:
            valor_dsva = calcular_variavel_dsva(self.configuracao_cenario.volume_financeiro_meta_ganhos, self.lista_cromossomo)
            valor_rppda = calcular_variavel_rppda(self.configuracao_cenario.volume_financeiro_risco_anual, self.lista_cromossomo)

            return calcular_funcao_minimizar_riscos_compra_energia(w1=self.configuracao_cenario.w1_penalizacao_desvio_negativo,
                                                                   dsva=valor_dsva,
                                                                   w2=1-self.configuracao_cenario.w1_penalizacao_desvio_negativo,
                                                                   rppda=valor_rppda)
        else:
            return get_maior_nota_avaliacao_disponivel(self.configuracao_cenario.tamanho_populacao)
    
    @property
    def ganhos_financeiro_melhor_cenario(self):
        return sum(gene.qtdade_energia_mwm_venda * gene.preco_pld * get_qtdade_horas_ano() for gene in self.lista_cromossomo)

    @property
    def risco_financeiro_cenario(self):
        return sum(gene.qtdade_energia_mwm_venda * gene.risco * get_qtdade_horas_ano() for gene in self.lista_cromossomo)

    def verificar_estouro_limite_qtdade_energia(self):
        self._estourou_limite_venda_energia = somar_qtdade_energia_cromossomos(self.lista_cromossomo) > self.configuracao_cenario.meta_anual_venda_kwm

        return self._estourou_limite_venda_energia
        

    def ajustar_cromossomo(self):
        """
        Ajusta a valor de venda de energia dos genes para que a soma anual não ultrapasse a meta de venda
        """        
        while somar_qtdade_energia_cromossomos(self.lista_cromossomo) > self.configuracao_cenario.meta_anual_venda_kwm:
            posicao_cromossomo_ajuste = round(random.random() * (len(self.lista_cromossomo) - 1))
            
            if self.lista_cromossomo[posicao_cromossomo_ajuste].qtdade_energia_mwm_venda > 0:
                self.lista_cromossomo[posicao_cromossomo_ajuste].qtdade_energia_mwm_venda -= 1

    def __gt__(self, other: object) -> bool:
        return not self.__eq__(other) and not self.__lt__(other)

    def __lt__(self, other: object) -> bool:
        """
        O objeto é considerado menor/pior que o outro quando:
        1 - Sua nota é maior que o outro objeto (pois a menor nota é a objetivo final porque queremos monimizar o risco)
        2 - ganhos_financeiro_melhor_cenario menor que o outro (queremos o cenário que tenha mais ganhos financeiros)
        3 - risco_financeiro_cenario mario que o outro (queremos o cenário com menor riscos financeiros)

        Args:
            other (Individuo): individuo de comparação

        Returns:
            bool: True se é menor
        """
               
        if self.nota_avaliacao == other.nota_avaliacao:
            if self.ganhos_financeiro_melhor_cenario == other.ganhos_financeiro_melhor_cenario:
                return self.risco_financeiro_cenario > other.risco_financeiro_cenario

            return self.ganhos_financeiro_melhor_cenario < other.ganhos_financeiro_melhor_cenario

        return self.nota_avaliacao > other.nota_avaliacao

    def __eq__(self, other: object) -> bool:
        return self.nota_avaliacao == other.nota_avaliacao \
                and self.ganhos_financeiro_melhor_cenario == other.ganhos_financeiro_melhor_cenario \
                and self.risco_financeiro_cenario == other.risco_financeiro_cenario
