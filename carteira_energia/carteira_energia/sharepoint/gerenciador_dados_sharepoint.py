import pandas as pd
from pandas import DataFrame
from carteira_energia.util.utilidades import get_qtdade_horas_ano
from carteira_energia.dao.informacoes_estudo_dao import InformacoesEstudoDAO
from infra_copel.sharepoint.site_copel import SharepointSiteCopel

class GerenciadorArquivosSharepoint(SharepointSiteCopel):
    INDICE_MES_INICIO_ESTUDO = 0
    INDICE_MES_FIM_ESTUDO = 11 
     
    """
    Classe genérica que gerencia a integração com o sharepooint
    """
    def __init__(self) -> None:
        super().__init__(site='PowerBIInsightsComercializacao')

    def salvar_conteudo_dataframe(self, nome_arquivo:str, conteudo_arquivo: DataFrame, index=False) -> None:
        """
        Salva o conteúdo do dataframe na pasta PLD

        Args:
            nome_arquivo (str): Nome do arquivo que será gerado
            conteudo_arquivo (DataFrame): Dataframe cujo conteúdo será utilizado para popular o arquivo a ser gerado
            index (bool, optional): O valor do index do dataframe será colocado no arquivo?. Defaults to False.
        """        
        self.write_df_to_excel(dataframe=conteudo_arquivo, folder=self._get_path_folder(), index=index, filename=nome_arquivo)
   
    def get_dataframe_cenarios_estudo(self) -> pd.DataFrame:
        """
        Caso existe uma planilha contendo uma lista de estudos que deverão ser plotados no relatório Power BI, um dataframe com essas informações será gerado.
        Caso não exista, um dataframe vazio será gerado

        Returns:
            pd.DataFrame: Dataframe com uma lista de nome de cenários
        """
        nome_arquivo = 'Lista cenarios estudo.xlsx'
        
        folder = self.get_folder(self._get_path_folder())
        if nome_arquivo in [arquivo['Name'] for arquivo in folder.files]:
            return self.read_df_from_excel(filename=nome_arquivo, folder=folder)
        else:
            return pd.DataFrame()
    
    def _get_path_folder(self) -> str:
        return 'Base de dados/'

class GerenciadorArquivosPLDSharepoint(GerenciadorArquivosSharepoint):
    """
    Classe específica que trata os arquivos relacionados a aba de PLD do Power BI
    """
    
    def _get_path_folder(self) -> str:
        return 'Base de dados/Newave-Decomp/'

    def get_dataframe_media_meses_ano(self) -> pd.DataFrame:
        # TODO: Esse método é utilizado pela Carteira de Compra e Venda de energia
        # No futuro devemos alterar esse método para obter os PLDs dos anos A+2 em diante
        # Como estamos gerando o PLD apenas para o ano A+1 (de forma fixa), estou me preocupando apenas com esse ano nesse momento
        # Sugestão para o futuro: Podemos acrescentar uma propriedade ano no construtor da classe

        """
        Obtêm a media mensal dos valores do PLD para o ano A+1

        Returns
        -------
        pd.DataFrame
            Dataframe contendo a média anual total do PLD
        """

        df = self.read_df_from_excel(filename='PLD.xlsx', folder=f'{self._get_path_folder()}PLD')

        df = pd.concat([df.loc[self.INDICE_MES_INICIO_ESTUDO:self.INDICE_MES_FIM_ESTUDO,['mes','valor_avg']].rename(columns={'valor_avg':'valor'}),
                        df.loc[self.INDICE_MES_INICIO_ESTUDO:self.INDICE_MES_FIM_ESTUDO,['mes','valor_p10']].rename(columns={'valor_p10':'valor'}),
                        df.loc[self.INDICE_MES_INICIO_ESTUDO:self.INDICE_MES_FIM_ESTUDO,['mes','valor_p25']].rename(columns={'valor_p25':'valor'}),
                        df.loc[self.INDICE_MES_INICIO_ESTUDO:self.INDICE_MES_FIM_ESTUDO,['mes','valor_p50']].rename(columns={'valor_p50':'valor'}),
                        df.loc[self.INDICE_MES_INICIO_ESTUDO:self.INDICE_MES_FIM_ESTUDO,['mes','valor_p75']].rename(columns={'valor_p75':'valor'})])

        return df.groupby(by='mes').agg({'valor':'mean'}).reset_index().rename(columns={'mes':'MES','valor':'VALOR'})

    def get_valores_pld_cenario(self, desc_cenario: str) -> pd.DataFrame:
        """
        Obtêm os valores de simulação do PLD para o cenário informados.
        Valores esperados: P10, P25, P50, P75 e AVG

        Returns:
            pd.DataFrame: DataFrame contendo os valores de simulação do PLD para o cenário informado 
        """
        
        df = self.read_df_from_excel(filename='PLD.xlsx', folder=f'{self._get_path_folder()}PLD')

        nome_coluna = f'valor_{desc_cenario.lower()}'

        return df.loc[self.INDICE_MES_INICIO_ESTUDO:self.INDICE_MES_FIM_ESTUDO,['mes',nome_coluna]].rename(columns={nome_coluna:'VALOR', 'mes':'MES'})

class GerenciadorArquivosEARSharepoint(GerenciadorArquivosSharepoint):
    """
    Classe específica que trata os arquivos relacionados a aba de EAR do Power BI
    """
        
    def _get_path_folder(self) -> str:
        return 'Base de dados/Newave-Decomp/EAR/'

class GerenciadorArquivosDcideSharepoint(GerenciadorArquivosSharepoint):
    """
    Gerenciador dos arquivos do sharepoint utilizados pelas abas do dcide do Power BI 

    Parameters
    ----------
    GerenciadorArquivosSharepoint : GerenciadorArquivosSharepoint
        classe base
    """    
    def __init__(self, ano_dcide: int) -> None:
        super().__init__()

        self.ano_dcide = ano_dcide
        
    def _get_path_folder(self):
        return f'Base de dados/Newave-Decomp/A+{self.ano_dcide}/'

class GerenciadorArquivosSharepointPortifolioRecomendacao(GerenciadorArquivosSharepoint):
    """
    Gerenciador especpifico para obtenção dos arquivos relacionados ao relatório recomendação de portfolio de venda de energia

    Args:
        GerenciadorArquivosSharepoint (GerenciadorArquivosSharepoint): Classe base
    """

    # TODO Por enquanto a silumação da carteira de venda de energia será fixo em A+1. No futuro temos que implementar A+2, A+3 e A+4
    # Isso depende do ajuste da obtenção do PLD A+2 em diante
    def __init__(self, ano_simulacao: int = 1) -> None:
        super().__init__()

        self.ano_simulacao = ano_simulacao
        self.dao = InformacoesEstudoDAO(sharepoint=GerenciadorArquivosPLDSharepoint())

    def _get_path_folder(self):
        return f'Base de dados/Portfólios Recomendados/A+{self.ano_simulacao}/'

    def get_dataframe_portfolio_ano(self) -> pd.DataFrame:
        """
        Obtêm o dataframe com os dados da meta do portfólio de recomendação do ano em questão

        Args:
            ano_simulacao (int): para qual ano os dados simulados devem ser obtidos

        Returns:
            pd.DataFrame: DataFrame contendo as metas do portifólio de um determinado ano
        """

        return self.read_df_from_excel(filename=f'Meta - Risco A+{self.ano_simulacao}.xlsx', folder=self._get_path_folder())

    def exportar_resultado_carteira_recomendacao(self, lista_individuos_exportacao: list):
        qtdade_horas_ano = get_qtdade_horas_ano()

        nome_coluna_ano = F'A+{self.ano_simulacao}'
        nome_coluna_mes = 'Mês'
        nome_coluna_valor = 'Valor'
        nome_coluna_melhor_preco = 'Melhor preço (R$)'
        nome_coluna_risco_preco = 'Preço risco (R$)'

        conteudo_dataframe = []
        mes_referencia = self.dao.get_data_inicial_estudo()
        for indice_individuo, individuo in enumerate(lista_individuos_exportacao, start=1):
            for gene in individuo.lista_cromossomo:
                conteudo_dataframe.append(
                    {
                        nome_coluna_ano: f'I{indice_individuo}',
                        nome_coluna_mes: f'{mes_referencia + gene.sequencia_mes}',
                        nome_coluna_valor: gene.qtdade_energia_mwm_venda,
                        nome_coluna_melhor_preco: round(gene.qtdade_energia_mwm_venda * gene.preco_pld * qtdade_horas_ano),
                        nome_coluna_risco_preco: round(gene.qtdade_energia_mwm_venda * gene.risco * qtdade_horas_ano)
                    }
                )

        self.salvar_conteudo_dataframe(nome_arquivo=f'portfolio A+{self.ano_simulacao}.xlsx', conteudo_arquivo=pd.DataFrame(conteudo_dataframe))