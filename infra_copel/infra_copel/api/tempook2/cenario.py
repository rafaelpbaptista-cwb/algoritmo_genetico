"""Criação de cenários na TempoOK."""

import logging
import re

import pandas as pd
import requests
import urllib.request

from infra_copel.api.tempook2.models import create_cenario_model
from infra_copel.config import get_variable

logger = logging.getLogger(__name__)


class Cenario:

    def __init__(
        self,
        modelos: list[str],
        qtde_dias: int | None = None,
        nome: str | None = None,
        prevs: bool = True,
        favorite: bool = False,
    ):

        # Token obtido do config.ini ou do Airflow
        self._token = get_variable("token_api_tempook2")
        # Modelos disponíveis, horizonte em dias e data da última rodada
        self._df_models = self.get_models_info_df()
        # Dataframe com os cenários, semelhante ao que aparece no site da TempoOK
        self._df_cenarios = self.montar_df_cenarios(
            modelos=modelos, qtde_dias=qtde_dias
        )

        # TODO: melhorar Dados necessários para a API da TempoOK
        self._nome = nome if nome else modelos[-1] + "teste"
        # Se não enviado na API o padrão é False (não gerar prevs)
        # mas para nossa utilização o padrão é True
        self._prevs = prevs
        # Não precisa deixar como favorito (padrão False)
        self._favorite = favorite

    @property
    def df_models(self):
        return self._df_models.copy()

    @property
    def df_cenarios(self):
        return self._df_cenarios.copy()

    def to_json(self):
        """Gera um objeto no estilo json, no estilo aceito pela TempoOK."""

        # Cria o Model do cenário
        # É gerado aqui e não no construtor, caso haja alguma alteração
        # manual no _df_cenários (geralmente para teste, já que falta
        # um validador)
        model = create_cenario_model(
            df=self._df_cenarios,
            name=self._nome,
            prevs=self._prevs,
            favorite=self._favorite,
        )

        # Pode depender da versão da biblioteca pydantic
        # para a versão 1.x
        return model.dict()
        # para a versão 2.x
        # return model.model_dump()

    def get_models_info_df(self):
        """
        Consulta na API o horizonte e data de atualização dos modelos.

        Adaptado do exemplo da TempoOK.

        """
        url = "https://cenario-beta.tempook.com/models"

        headers = {"Authentication": self._token}

        response = requests.get(
            url, headers=headers, allow_redirects=False, verify=False
        )

        if response.status_code != 200:
            raise Exception("ERRO")

        models_list = response.json()
        df = pd.DataFrame(models_list)

        df["last_run"] = pd.to_datetime(df["last_run"], format="%d/%m/%Y")
        df["last_run"] = df["last_run"].dt.to_period("D")

        # TODO: Conferir, talvez tenham modelos que tenha que adicionar ou diminuir 1
        df["max_day"] = df["last_run"] + df["days_available"]

        return df.set_index("model")

    def montar_df_cenarios(self, modelos: list[str], qtde_dias: int) -> pd.DataFrame:
        """
        Montagem do dataframe de cenário, semelhante ao site da TempoOK.

        Parameters
        ----------
        modelos : list[str]
            Lista de modelos a serem utilizados.
        qtde_dias : int
            Quantidade de dias do cenário.

        Returns
        -------
        DataFrame
            Tabela com os cenários montados.

        Raises
        ------
        Exception
            Caso dê alguma merda.
        """

        hoje = pd.Timestamp("today").to_period("D")

        range_datas = pd.period_range(start=hoje + 1, periods=qtde_dias)

        serie = pd.Series([None] * len(range_datas), index=range_datas)

        for modelo in modelos:
            max_day = self._df_models.loc[modelo, "max_day"]
            # Para não extrapolar a serie desejada
            day = min(serie.index[-1], max_day)
            serie.loc[day] = modelo

        # Preenche a série de trás para frente
        # (do dia máximo de cada modelo até onde haja outro preenchimento)
        serie_preenchida = serie.bfill()

        # Não permite a continuidade caso haja algum dia sem modelo
        if serie_preenchida.isna().any():
            raise Exception("Modelos não conseguem contemplar todo o período")

        # Método para realizar a virada de ano
        serie_preenchida = self.mudar_modelo_ano(serie_preenchida)

        # Transformação em dataframe de um submercado
        df_submercado = serie_preenchida.to_frame("model")
        df_submercado["lim"] = None
        df_submercado["factor"] = 1

        # Replicando para todos os submercados
        submercados_df = {
            "Sudeste": df_submercado,
            "Sul": df_submercado,
            "Nordeste": df_submercado,
            "Norte": df_submercado,
        }
        df = pd.concat(submercados_df, axis=1)

        return df

    def mudar_modelo_ano(self, series: pd.Series) -> pd.Series:

        series = series.copy()
        viradas = series[series.index.year != series.index.shift(-1).year].index

        for dia_01_01 in viradas:
            dia_31_12 = dia_01_01 - 1
            modelo_31_12 = series[dia_31_12]
            modelo_01_01 = series[dia_01_01]
            logger.info(f"Em {dia_31_12}, o modelo é {modelo_31_12}")
            logger.info(f"Em {dia_01_01}, o modelo é {modelo_01_01}")
            if match := re.match(r"ANO-(\d{4})", modelo_31_12):
                ano_31_12 = int(match.group(1))

                if match := re.match(r"ANO-(\d{4})", modelo_01_01):
                    ano_01_01 = int(match.group(1))

                    if ano_01_01 == ano_31_12:
                        logger.info("Realizando virada")
                        novo_ano = ano_31_12 + 1
                        novo_ano = f"ANO-{novo_ano}"
                        if novo_ano not in self._df_models.index:
                            raise Exception(f"Modelo {novo_ano} não existe")
                        logger.info("Novo modelo: %s", novo_ano)
                        series.loc[dia_01_01:] = novo_ano

        return series

    def enviar(self):
        """Envia para a TempoOK via API."""

        # definindo a url do endpoint
        url = "https://cenario-beta.tempook.com/execute"

        # cabeçalho com o token de autenticação
        headers = {"authentication": self._token}  # substitua pelo seu token real

        # fazendo a requisição post com dados json
        response = requests.post(
            url,
            json=self.to_json(),
            headers=headers,
            allow_redirects=False,
            verify=False,
        )

        if response.status_code != 200:
            return response.text

        self._response = response.json()

        return response.json()

    def download_prevs(self, destino):

        if self._response:
            url = self._response["prevs_url"]
            urllib.request.urlretrieve(url=url, filename=destino)
