import pandas as pd

#TODO No futuro as informações aqui colhidas serão obtidas do MongoDB quando o processo automatizado do Rolling salvar informações na base de dados
class InformacoesEstudoDAO():

    def __init__(self, sharepoint) -> None:
        """
        Construtor da classe

        Parameters
        ----------
        sharepoint : GerenciadorArquivosPLDSharepoint
            Sharepoint para obter as informações de pevisão do PLD oriundas do estudo do Rolling
        """        
        self.sharepoint_pld = sharepoint

    def get_data_inicial_estudo(self) -> pd.Period:
        """
        Obtêm a data inicial do estudo Rolling

        Returns
        -------
        pd.Period
            Data inicial do resultado do estudo
        """
        df = self.sharepoint_pld.get_valores_pld_cenario('P10')

        return pd.Period(df.iloc[0,0])