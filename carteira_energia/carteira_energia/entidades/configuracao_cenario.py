from dataclasses import dataclass, field
from carteira_energia.formulas.formulas_agoritmo_genetico import calcular_variavel_prisco
from carteira_energia.sharepoint.gerenciador_dados_sharepoint import GerenciadorArquivosSharepointPortifolioRecomendacao, GerenciadorArquivosPLDSharepoint

MSG_ERRO_CENARIO_P75_VAZIO = 'Não foi encontrado valores para o cenário pencentil 75'

MSG_ERRO_CENARIO_MEDIA_VAZIO = 'Não foi encontrado valores de média dos cenários para O preço PLD'
@dataclass
class ConfiguracaoCenario():
    """
    Classe que armazena dados relacionados a configuração do cenário que será rodado

    Propriedades:
    - meta_anual_venda_kwm: campo do tipo int que representa a quantidade de MWm que deseja-se vender para um determinado ano.
    Esse é a meta de venda da diretoria

    - tamanho_populacao: campo do tipo int que representa a qtdade da população (possíveis respostas) que iremos trabalhar

    - lista_preco_pld_mes: list contendo os preços dos PLDs mensais oriundos da rodada do newave. Cada indice da lista representa um mês do ano.
    Essa lista tem um tamanho máximo de 12 (cada mês do ano)

    - lista_risco_mes: lista contendo a representaçaõ do risco de cada mês

    """
    
    ano_simulacao: int
    meta_anual_venda_kwm: int = 0
    volume_financeiro_meta_ganhos: int = 0
    volume_financeiro_risco_anual: int = 0
    
    lista_preco_pld_mes: list[int] = field(default_factory=list)
    lista_risco_mes: list[int] = field(default_factory=list)

    tamanho_populacao: int = 500
    limite_qtdade_geracoes_melhor_individuo: int = 5
    
    w1_penalizacao_desvio_negativo: int = 0.6

    taxa_mutacao: float = 0.15

    def __post_init__(self):
        sharepoint_portfolio_recomendacao = GerenciadorArquivosSharepointPortifolioRecomendacao()
        sharepoint_pld = GerenciadorArquivosPLDSharepoint()

        dataframe_informacoes_meta_anual = sharepoint_portfolio_recomendacao.get_dataframe_portfolio_ano()
        self.meta_anual_venda_kwm = dataframe_informacoes_meta_anual['Meta (MWmédio)'][0]
        self.volume_financeiro_meta_ganhos = dataframe_informacoes_meta_anual['VE Fat (R$^6)'][0] * 1000000
        self.volume_financeiro_risco_anual = dataframe_informacoes_meta_anual['RISK A (R$^6)'][0] * 1000000

        dataframe_media_previsao_preco_pld = sharepoint_pld.get_dataframe_media_meses_ano()
        dataframe_preco_cenarios_pld = sharepoint_pld.get_valores_pld_cenario('P75')

        self._validar_dataframe_coluna_vazia(dataframe=dataframe_preco_cenarios_pld, msg=MSG_ERRO_CENARIO_P75_VAZIO)
        self._validar_dataframe_coluna_vazia(dataframe=dataframe_media_previsao_preco_pld, msg=MSG_ERRO_CENARIO_MEDIA_VAZIO)

        dataframe_valor_risco_mes = calcular_variavel_prisco(dataframe_cenarios_pld=dataframe_preco_cenarios_pld,
                                                             dataframe_preco_pld_medio_mes=dataframe_media_previsao_preco_pld,
                                                             ano_simulacao=self.ano_simulacao)

        self.lista_preco_pld_mes = dataframe_media_previsao_preco_pld['VALOR'].to_list()
        self.lista_risco_mes = dataframe_valor_risco_mes['VALOR_RISCO'].to_list()

    def _validar_dataframe_coluna_vazia(self, dataframe, nome_coluna: str = 'VALOR', msg: str = 'Coluna VALOR está vazia'):
        if dataframe[nome_coluna].isnull().any():
            raise ValueError(msg)
