from carteira_energia.entidades.algoritmo_genetico import AlgoritmoGenetico
from carteira_energia.sharepoint.gerenciador_dados_sharepoint import GerenciadorArquivosSharepointPortifolioRecomendacao


class CarteiraCompraVendaEnergia:
    """
    Classe que representa uma carteirta de sugestao de compra e venda de energia eletrica
    """

    def __init__(self, ano_simulacao: int):
        self.algoritmo = AlgoritmoGenetico(ano_simulacao)

    def encontrar_recomendacao(self) -> list:
        """
        Executa o algoritmo para encontrar os 3 melhores indivíduos com sugestão de carteira de venda de energia 

        Returns:
            list: lista contendo as 3 melhores carteiras de venda de energia
        """

        self.algoritmo.executar()

        return  self.algoritmo.get_lista_melhores_individuos()
        
    def exportar_resultado_pasta_sharepoint(self):
        """
        Exporta conteúdo dos 3 melhores indivíduos no diretório do sharepoint para relatório Power BI exibir os seus dados
        """
        
        GerenciadorArquivosSharepointPortifolioRecomendacao() \
                    .exportar_resultado_carteira_recomendacao(lista_individuos_exportacao=self.algoritmo.get_lista_melhores_individuos()[0:3])
