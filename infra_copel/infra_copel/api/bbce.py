import requests
import pandas as pd
import re
import locale
import time
from urllib3.exceptions import InsecureRequestWarning
import logging

# Set locale name
# Usado para ler nome dos meses em pt_BR
locale.setlocale(locale.LC_ALL, "pt_BR.UTF-8")
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


class ApiBBCE:
    def __init__(self, email_bbce: str, senha_bbce: str, api_key: str, url_base: str):
        """
        Construtor.

        Parameters
        ----------
        email_bbce : str
            email de acesso BBCE.

        senha_bbce : str
            senha de acesso BBCE

        api_key : str
            chavede acesso da api da BBCE

        url_base : str
            url base do acesso das apis
            exemplo: "https://api-ehub.bbce.com.br/bus"

        self.token
            poteriormente garante que o codigo todo utiliza o mesmo token de autenticação

        """
        self.email_bbce = email_bbce
        self.senha_bbce = senha_bbce
        self.api_key = api_key
        self.url_base = url_base
        self.token = self._get_token()
        self.header = self._get_header_default()

    def _get_token(self) -> str:
        """
        Função que pega o token de acesso a api e utiliza apenas um token

        Returns
        -------
        my_token
           token de acesso
        """

        dados = {
            "email": f"{self.email_bbce}",
            "password": f"{self.senha_bbce}",
            # Código de acesso da Copel
            "companyExternalCode": 1472,
        }

        headers = {
            "accept": "application/json",
            "apiKey": f"{self.api_key}",
            "Content-Type": "application/json",
        }

        url_login = f"{self.url_base}/v2/login"
        logging.info("Gerando o o Token de acesso da api")

        response = requests.post(
            url_login, headers=headers, json=dados, verify=False, timeout=60
        )

        status_conta = response.json()

        return status_conta["idToken"]

    def _get_header_default(self) -> dict:
        """
        Função que monta o header utilizados nos requests

        Returns
        -------
        headers_default
           um dicionario com os parametros necessarios para se conectar na api da bbce
        """

        headers_default = {
            "accept": "application/json",
            "apiKey": f"{self.api_key}",
            "Authorization": f"Bearer {self.token}",
        }

        return headers_default

    def fetch_data(self, url, retries=7) -> dict:
        """
        Função que faz os requests de e respeita os timeout da api

        Parameters
        ----------
        url
            url pra fazer o request

        retries
            o numero maximo de tentativas do request

        Returns
        -------
        response.json()
           um json com a resposta do request
        """

        for i in range(retries):
            response = requests.get(url, headers=self.header, verify=False, timeout=60)

            if response.status_code == 429:  # 429 Muitas requests
                wait_time = 2**i  # Tempo de espera exponencial
                logging.info(
                    "Limite de conexões excedido. Esperando por %i segundos.", wait_time
                )

                time.sleep(wait_time)
            else:
                response.raise_for_status()

                return response.json()
            
        raise Exception("Máximo número de tentativas excedido")

    def filtra_str(self, string: str) -> bool:
        """
        Função que filtra se uma string contem [SE,CON e Preço Fixo]
        e se não tem OTR

        Parameters
        ----------
        string : str
            String a ser filtrada.

        Returns
        -------
        bool
           valor booleano se a string atende ou não aos filtros impostos
        """

        # Filtro de inclusão na string
        inclusao = bool(re.search(r"SE.*CON.*Preço Fixo", string, re.IGNORECASE))

        # Filtro de exclusâo na string
        exclusao = bool(re.search(r"OTR", string, re.IGNORECASE))

        # volta true apenas se inclusao for True e exclusao for False
        return inclusao and not exclusao

    def get_tickers(self) -> pd.DataFrame:
        """
        Função que baixa uma tabela com todos os tickers existentes na BBCE

        Returns
        -------
        df_ticker
           o dataframe com todos os tickers
        """

        url_tickers = f"{self.url_base}/v2/tickers"

        logging.info("Obtendo dados de todos os tickers na API BBCE")
        df_ticker = self.fetch_data(url_tickers)
        df_ticker = pd.DataFrame(df_ticker)
        df_ticker = df_ticker.sort_values(by="id")
        df_ticker.reset_index(inplace=True, drop=True)
        
        return df_ticker

    def get_tickers_ativos(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Função que filtra o df com todos os tickers e mantem apenas os que estão ativos

        Parameters
        ----------
        df
            é o dataframe com todos os tickers existentes
        Returns
        -------
        tickers_ativos
           o dataframe com todos os tickers que estão ativos
        """
        tickers_ativos = df[df["active"]]
        tickers_ativos.reset_index(inplace=True, drop=True)

        return tickers_ativos

    def filtra_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Função que filtra o df com todos os tickers ativos utilizando a função filtra_str()

        Parameters
        ----------
        df
            é o df com os todos tickers ativos
        Returns
        -------
        tickers_ativos
           o dataframe com todos os tickers que estão ativos
        """

        df["filtro"] = df["description"].apply(self.filtra_str)
        df_filtrado = df[df["filtro"]]
        
        df_filtrado.reset_index(inplace=True, drop=True)

        return df_filtrado

    def get_precos(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Função que pega monta um dataframe com os preços dos tickers APÓS os dataframes serem filtrados

        Parameters
        ----------
        df
            dataframe filtrado (apenas com os tickers requisitados)

        Returns
        -------
        df_preco_filltrado
           o dataframe com todos os preços para os dos tickers ativos e filtrafos
        """

        lista = []

        for _, linha in df.iterrows():
            url_preco = f"{self.url_base}/v1/negotiation-data/{linha['id']}"
            logging.info("Obtendo dados API BBCE ticker de numero %s", linha["id"])

            prec = self.fetch_data(url_preco)

            prec["id"] = linha["id"]
            prec["description"] = linha["description"]

            lista.append(prec)
            # transforma a lista de preços em um DataFrame

        df_preco_filltrado = pd.DataFrame(lista)

        return df_preco_filltrado

    def renomeia_df(self, df: pd.DataFrame) -> None:
        """
        Função que renomeia as colunas do dataframe com os preços

        Parameters
        ----------
        df
            dataframe com os preços

        Returns
        -------
        df
           o dataframe com os nomes das colunas trocados
        """
        if "lastPriceDate" in df.columns:
            df["lastPriceDate"] = pd.to_datetime(df["lastPriceDate"])

        df["date"] = pd.to_datetime(df["date"])
        df["date"] = df["date"].dt.normalize()
        df["date"] = pd.to_datetime(df["date"])

        df.rename(
            columns={
                "date": "data_download",
                "minPrice": "preco_minimo",
                "maxPrice": "preco_maximo",
                "firstPrice": "primeiro_preco",
                "lastPrice": "ultimo_preco",
                "lastPriceDate": "ultima_data_preco",
                "id": "ticker_id",
                "description": "descricao",
            },
            inplace=True,
        )
       