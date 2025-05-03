import pytest
from carteira_energia.entidades.algoritmo_genetico import ConfiguracaoCenario
from carteira_energia.entidades.gene_representacao_mes import GeneRepresentacaoMes
from carteira_energia.entidades.individuo import Individuo
from carteira_energia.entidades.algoritmo_genetico import AlgoritmoGenetico

META_ANUAL_VENDA_KWM = 14
VOLUME_RISCO_ANUAL = 5000000
VOLUME_META_ANUAL = 3000000000
TAMANHO_POPULACAO = 1000

PRECO_PLD = 200
RISCO_MES = 90
QTDADE_VENDA_ENERGIA_MES = round(META_ANUAL_VENDA_KWM / 12)

QTDADE_MESES_ANO = 12

@pytest.fixture
def configuracao_cenario():
    configuracao = ConfiguracaoCenario(meta_anual_venda_kwm=META_ANUAL_VENDA_KWM,
                                       volume_financeiro_meta_ganhos= VOLUME_META_ANUAL,
                                       volume_financeiro_risco_anual=VOLUME_RISCO_ANUAL, 
                                       tamanho_populacao=TAMANHO_POPULACAO,
                                       ano_simulacao=1)
    configuracao.lista_preco_pld_mes = [100, 200, 300, 90, 100, 200, 100, 200, 300, 90,	100, 200]
    configuracao.lista_risco_mes = [90,	85,	70,	90,	80,	110, 90, 120, 150, 90, 100,	70]

    return configuracao

@pytest.fixture
def gene_representacao_mes():
    return  GeneRepresentacaoMes(sequencia_mes=1, preco_pld=PRECO_PLD, risco=RISCO_MES, qtdade_energia_mwm_venda=QTDADE_VENDA_ENERGIA_MES)

@pytest.fixture
def individuo(configuracao_cenario: ConfiguracaoCenario, gene_representacao_mes: GeneRepresentacaoMes):
    individuo_aux = Individuo(configuracao_cenario=configuracao_cenario, gerar_cromossomo=False)
    individuo_aux.lista_cromossomo = [gene_representacao_mes for indice in range(QTDADE_MESES_ANO)]

    return individuo_aux

@pytest.fixture
def melhor_individuo(configuracao_cenario: ConfiguracaoCenario):
    melhor_individuo_aux = Individuo(configuracao_cenario=configuracao_cenario)
    
    melhor_individuo_aux.lista_cromossomo = []

    for mes in range(QTDADE_MESES_ANO):
        melhor_individuo_aux.lista_cromossomo.append(GeneRepresentacaoMes(sequencia_mes=mes, preco_pld=PRECO_PLD + 100, risco=RISCO_MES - 10, qtdade_energia_mwm_venda=QTDADE_VENDA_ENERGIA_MES))

    return melhor_individuo_aux

@pytest.fixture
def algoritmo_genetico(configuracao_cenario: ConfiguracaoCenario, individuo: Individuo, melhor_individuo: Individuo):
    algoritmo_genetico_aux = AlgoritmoGenetico(ano_simulacao=1)
    algoritmo_genetico_aux.configuracao_cenario = configuracao_cenario

    algoritmo_genetico_aux.lista_populacao = [individuo for indice in range(QTDADE_MESES_ANO)]
    algoritmo_genetico_aux.lista_populacao[11] = melhor_individuo

    return algoritmo_genetico_aux