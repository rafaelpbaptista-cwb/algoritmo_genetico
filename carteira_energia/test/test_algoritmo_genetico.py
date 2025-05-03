import test.conftest as conftest
from carteira_energia.entidades.algoritmo_genetico import AlgoritmoGenetico
from carteira_energia.entidades.configuracao_cenario import ConfiguracaoCenario
from carteira_energia.entidades.individuo import Individuo
from carteira_energia.util.utilidades import somar_qtdade_energia_cromossomos

def test_criar_algoritmo_genetico():
    qtdade_meses_ano = 12

    algoritmo = AlgoritmoGenetico(ano_simulacao=1)

    assert algoritmo.configuracao_cenario.tamanho_populacao == len(algoritmo.lista_populacao), f'O tamanho da populacao deve ser igual a {algoritmo.configuracao_cenario.tamanho_populacao}'

    for individuo in algoritmo.lista_populacao:
        assert len(individuo.lista_cromossomo) == \
            qtdade_meses_ano, \
                f'O tamanho do cromossomo do individuo deve ser igual a {qtdade_meses_ano}. Foi {len(individuo.lista_cromossomo)}'
        
        if not individuo.verificar_estouro_limite_qtdade_energia():
            assert somar_qtdade_energia_cromossomos(individuo.lista_cromossomo) == \
                conftest.META_ANUAL_VENDA_KWM, \
                    f'A soma dos genes do cromossomo do individuo deve ser igual a {conftest.META_ANUAL_VENDA_KWM}'

def test_verificar_selecao_melhor_individuo(algoritmo_genetico: AlgoritmoGenetico, melhor_individuo: Individuo):
    algoritmo_genetico._avaliar_populacao()
    algoritmo_genetico._selecionar_melhores_individuos()

    assert algoritmo_genetico.melhor_individuo == melhor_individuo, 'Seleção de melhor indivíduo está com erro'

def test_qtdade_populacao_apos_crossover(configuracao_cenario: ConfiguracaoCenario):
    algoritmo = AlgoritmoGenetico(ano_simulacao=1)
    algoritmo.configuracao_cenario = configuracao_cenario

    algoritmo._crossover()

    assert len(algoritmo.lista_populacao) ==  configuracao_cenario.tamanho_populacao, \
            f'Tamanho da população após crossover não é o esperado. Valor {len(algoritmo.lista_populacao)} esperado {configuracao_cenario.tamanho_populacao}'

def test_verificar_cromossomos_nova_populacao_apos_crossover(configuracao_cenario: ConfiguracaoCenario):
    algoritmo = AlgoritmoGenetico(ano_simulacao=1)
    algoritmo.configuracao_cenario = configuracao_cenario

    algoritmo._crossover()
    
    for individuo in [indiduo_filtrado for indiduo_filtrado in algoritmo.lista_populacao if not indiduo_filtrado.verificar_estouro_limite_qtdade_energia()]:
        assert len(individuo.lista_cromossomo) <= conftest.QTDADE_MESES_ANO, \
            f'Tamanho do cromossomo do indivíduo não pode ultrapassar o valor de {conftest.QTDADE_MESES_ANO}. Valor atual é de {len(individuo.lista_cromossomo)}'
        
        assert somar_qtdade_energia_cromossomos(individuo.lista_cromossomo) <= conftest.META_ANUAL_VENDA_KWM, \
                f'A soma dos genes do cromossomo do individuo não pode ultrapassar o valor de {conftest.META_ANUAL_VENDA_KWM}. Valor atual foi de {somar_qtdade_energia_cromossomos(individuo.lista_cromossomo)}'