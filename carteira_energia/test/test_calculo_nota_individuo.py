from carteira_energia.entidades.individuo import Individuo
from carteira_energia.entidades.configuracao_cenario import ConfiguracaoCenario

META_ANUAL_VENDA_KWM = 20
VOLUME_RISCO_ANUAL = 15000000
VOLUME_META_ANUAL = 50000000
TAMANHO_POPULACAO = 1000
QTDADE_MESES_ANO = 12
QTDADE_GERACOES = 40

def test_calculo_nota_individuo():
    configuracao = ConfiguracaoCenario(ano_simulacao=1)
    configuracao.meta_anual_venda_kwm = META_ANUAL_VENDA_KWM
    configuracao.volume_financeiro_meta_ganhos = VOLUME_META_ANUAL
    configuracao.volume_financeiro_risco_anual = VOLUME_RISCO_ANUAL
    configuracao.tamanho_populacao = TAMANHO_POPULACAO

    configuracao.lista_preco_pld_mes = [100, 200, 300, 90, 100, 200, 100, 200, 300, 150, 100, 200]
    configuracao.lista_risco_mes =     [90,	 85,  70,  70,	80,	110, 90,  120, 150, 80,  90,  70]

    individuo = Individuo(configuracao)
    individuo.lista_cromossomo[0].qtdade_energia_mwm_venda = 0
    individuo.lista_cromossomo[1].qtdade_energia_mwm_venda = 0
    individuo.lista_cromossomo[2].qtdade_energia_mwm_venda = 0
    individuo.lista_cromossomo[3].qtdade_energia_mwm_venda = 1
    individuo.lista_cromossomo[4].qtdade_energia_mwm_venda = 0
    individuo.lista_cromossomo[5].qtdade_energia_mwm_venda = 0
    individuo.lista_cromossomo[6].qtdade_energia_mwm_venda = 0
    individuo.lista_cromossomo[7].qtdade_energia_mwm_venda = 0
    individuo.lista_cromossomo[8].qtdade_energia_mwm_venda = 11
    individuo.lista_cromossomo[9].qtdade_energia_mwm_venda = 0
    individuo.lista_cromossomo[10].qtdade_energia_mwm_venda = 0
    individuo.lista_cromossomo[11].qtdade_energia_mwm_venda = 0

    assert individuo.nota_avaliacao == 12209040, 'Valor do calculo esta errado'
