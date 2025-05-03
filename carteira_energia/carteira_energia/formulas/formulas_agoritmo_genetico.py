from typing import List
from pandas import DataFrame
from carteira_energia.util.utilidades import get_qtdade_horas_ano
from carteira_energia.entidades.gene_representacao_mes import GeneRepresentacaoMes

QTDADE_HORAS_ANO = get_qtdade_horas_ano()

def calcular_funcao_minimizar_riscos_compra_energia(w1: float, dsva: float, w2: float, rppda: float) -> float:
    """
    Função minimizar riscos compra de energia
    Minimizar w_1 * Dsv_a + w_2 * Rppd_a

    Args:
        w1 (float): penalização atribuída a desvios negativos em relação ao valor esperado requerido pelo investidor
        dsva (float): desvio negativo em relação ao valor esperado, requerido pela diretoria. Valor em Reais (R$)
        w2 (float): penalização atribuída a desvios positivos em relação ao risco requerido pelo investidor
        rppda (float): desvio positivo em relação ao risco máximo aceito pela diretoria, Valor em Reais (R$)

    Returns:
        float: calculo da noto do indivíduo onde devemos encontrar o menor valor como resposta
    """

    return (w1 * dsva) + (w2 * rppda)

def calcular_variavel_dsva(verec: float, lista_cromossomo: List[GeneRepresentacaoMes]) -> float:
    """
    Obter variável $Dsv_a$ para um determinado gene

    Dsv_a = max [ VE_rec - sum_{m=1}^{M} (QC_m * PV_m); 0]

    QC_m$ - Volume em MWh a ser comercialzado em um dado mês
    $PV_m$ - Preço médio para um contrato anual estimado a partir do Rolling (R$/MWh)

    Args:
        verec (int): Valor esperado pela diretoriareferente a receita com contrator bilaterais em milhões de Reais para um dado ano
        lista_cromossomo (List[ConfiguracaoCenario]): Lista de cromossos do individuo

    Returns:
        float: Valor da variável DSVA a ser utlizada no cálculo da nota do indivíduo
    """

    soma_multiplicacao_qcm_pvm = sum(gene.qtdade_energia_mwm_venda * gene.preco_pld * QTDADE_HORAS_ANO for gene in lista_cromossomo)

    return max((verec - soma_multiplicacao_qcm_pvm), 0)

def calcular_variavel_rppda(riskppd: float, lista_cromossomo: List[GeneRepresentacaoMes]) -> float:
    """
    Obter variável $Rppd_a$ para um determinado gene

    Rppd_a = max [ sum_{m=1}^{M} (Prisco_m * QC_m) - RISK_ppd); 0 ]

    Args:
        riskppd (int): Risco máximo ao faturamento aceio pela diretoria.
        lista_cromossomo (List[ConfiguracaoCenario]): Lista de cromossos do individuo

    Returns:
        float: Valor da variável RPPDA a ser utlizada no cálculo da nota do indivíduo
    """

    soma_multiplicacao_prisco_qcm = sum(gene.qtdade_energia_mwm_venda * gene.risco * QTDADE_HORAS_ANO for gene in lista_cromossomo)

    return max(soma_multiplicacao_prisco_qcm - riskppd, 0)

def calcular_variavel_prisco(dataframe_preco_pld_medio_mes: DataFrame, dataframe_cenarios_pld: DataFrame, ano_simulacao: int) -> DataFrame:
    """
    Calculo da variável Prisco

    Args:
        dataframe_preco_pld_medio_mes (DataFrame): _description_
        dataframe_cenarios_pld (DataFrame): _description_

    Returns:
        DataFrame: Dataframe contendo o cáculo da varável Prisco
    """
    qtdade_meses_ano = 12

    dataframe_valor_minimo_mes = dataframe_cenarios_pld[['MES', 'VALOR']].groupby('MES').min()
    dataframe_valor_minimo_mes.rename(columns={'VALOR': 'VALOR_PLD_MINIMO'}, inplace=True)

    dataframe_risco_mes = dataframe_valor_minimo_mes.merge(dataframe_preco_pld_medio_mes[['MES', 'VALOR']], on='MES')
    dataframe_risco_mes.rename(columns={'VALOR': 'VALOR_MEDIA_PLD_CENARIOS'}, inplace=True)
    
    dataframe_risco_mes['NUMERO_MES_ESTUDO'] = (qtdade_meses_ano * ano_simulacao) - (qtdade_meses_ano - (dataframe_risco_mes.index + 1))
    dataframe_risco_mes['VALOR_RISCO'] = dataframe_risco_mes['VALOR_MEDIA_PLD_CENARIOS'] - dataframe_risco_mes['VALOR_PLD_MINIMO'] * (1 - dataframe_risco_mes['NUMERO_MES_ESTUDO'] / 100)

    return dataframe_risco_mes

