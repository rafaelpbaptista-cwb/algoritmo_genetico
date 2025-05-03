import tempfile
import urllib.request
import logging
import requests
import os
import tarfile
import pandas as pd
from typing import Optional
from datetime import datetime, timedelta
from infra_copel import SharepointSiteCopel
from infra_copel.config import get_variable
from xlcalculator.xlfunctions.func_xltypes import Blank
from openpyxl import load_workbook
from io import BytesIO
from xlcalculator import ModelCompiler
from xlcalculator import Model
from xlcalculator import Evaluator

SITE_SHAREPOINT = "PowerBIInsightsComercializacao"
FOLDER_CONFIG = "Base de Dados/Configuracao Cenarios Prevs TempoOK"
FILE_CONFIG_LONGO_PRAZO = "Configuracao Cenarios Prevs TempoOK LongoPrazo.xlsx"
FILE_CONFIG_CURTO_PRAZO = "Configuracao Cenarios Prevs TempoOK CurtoPrazo.xlsx"
URL_DADOS_PREVS = "https://cenario-beta.tempook.com/execute"


def _get_header_auth() -> dict:
    """
    Obtem o token de autenticação para o uso da API da TempoOK.

    Returns
    -------
    dict
        Dados do token de autenticação.
    """
    return {"Authentication": get_variable("token_api_tempook")}


def criar_cenario(data: dict) -> dict:
    """
    Cria um cenário com base nos dados fornecidos e retorna um dicionário com o resultado do tratamento do cenário.

    Parameters:
        data (dict): Um dicionário contendo os dados necessários para criar o cenário.

    Returns:
        dict: Um dicionário contendo o resultado do tratamento do cenário.
    """

    logging.info("Cenário: %s", data["name"])

    response = requests.post(URL_DADOS_PREVS, json=data, headers=_get_header_auth(), verify=False)
    dicionario = response.json()
    dicionario["name"] = data["name"]

    logging.info("Fim tratamento cenário %s.", data["name"])

    return dicionario


def get_cenarios_vazoes_customizaveis(
    tipo_prevs: str,
    ambiente_hml: bool,
    filename_config: str,
    sheet_name: Optional[str] = None,
) -> dict:
    """
    Cria uma lista de jsons contendo a configuração para a execução de um cenário prevs

    Parameters
    ----------
    tipo_prevs : str
        Tipo do cenario

    ambiente_hml : bool
        Indica que o process está sendo rodado no ambiente de HML
        Isso irá acrescentar a string HML no nome do cenario

    filename_config: str
        Caminho para o arquivo de configuração

    sheet_name : Optional[str]
        Nome da aba da planilha excel onde está localizado a configuração para o cenario. Se não fornecido, será lido o conteúdp da primeira aba.

    vies : str
        Valores aceitados pela TempoOK: com e sem

    Returns
    -------
    dict
        Configuração para a execução de um cenário prevs
    """
    df_config = _criar_df_config(filename_config, sheet_name)

    return _criar_dict_config(
        lista_config_prevs_dias=_criar_config_prevs_por_dia(
            df_config=df_config, tipo_prevs=tipo_prevs
        ),
        tipo_prevs=tipo_prevs,
        ambiente_hml=ambiente_hml,
    )


def _criar_dict_config(
    lista_config_prevs_dias: list,
    tipo_prevs: str,
    ambiente_hml: bool,
) -> dict:
    """
    Cria um dicionário contendo a configuração para a execução de um cenário prevs
    Esse dict será usado na chamada da API da TempoOK

    Parameters
    ----------
    lista_config_prevs_dias : list
        Lista de confguração para cada dia da previsão. Para cada dia necessito indicar qual modelo de prevs deve ser usado
    tipo_prevs : str
        Tipo da prevs: AVG, P25 ou P50
    ambiente_hml : bool
        Indica que o process está sendo rodado no ambiente de HML
        Isso irá acrescentar a string HML no nome do cenario

    Returns
    -------
    dict
        Dicionario contendo a configuração para a execução de um cenário prevs
    """

    nome_cenario = f"{tipo_prevs}-{datetime.now().strftime('%Y%m%d')}"

    if ambiente_hml:
        nome_cenario = f"HML-{nome_cenario}"

    config = {
        "name": nome_cenario,
        "favorite": False,
        "prevs": True,
        "configuration": lista_config_prevs_dias,
    }

    return config


def _criar_df_config(filename_config: str, sheet_name: str) -> pd.DataFrame:
    """
    Lê, do sharepoint, planilha excel contendo quais modelos de previsão devem ser usados para cada dia.
    Segue um exemplo do formato experado para essa planilha:
    +------------+------------+-------------+---------------+-------------+-------------+-------------+-------------+-------------+-------------++----------+----------+-----------+------+
    | Data_Ini   | Data_Fim   | qtdade_dias | ANO_2007-2%   | ANO_2010-2% | ANO_2016-2% | ANO_2017-2% | ANO_2019-2% | ANO_2020-2% | ANO_2021-2% | FATOR_SE | FATOR_S | FATOR_NE | FATOR_N |
    +------------+------------+-------------+---------------+-------------+-------------+-------------+-------------+-------------+-------------+----------------------+----------+-------+
    | 16/07/2024 | 29/07/2024 | 13          | ANO-2007      | ANO-2010    | ANO-2016    | ANO-2017    | ANO-2019    | ANO-2020    | ANO-2021    | 1        | 1        | 1         | 1     |
    +------------+------------+-------------+---------------+-------------+-------------+-------------+-------------+-------------+-------------+----------------------+----------+-------+
    | 30/07/2024 | 31/12/2024 | 154         | ECENSav-ETA40 | ANO-2011    | ANO-2017    | ANO-2018    | ANO-2020    | ANO-2021    | ANO-2022    | 1        | 1        | 1        | 1      |
    +------------+------------+-------------+---------------+-------------+-------------+-------------+-------------+-------------+-------------+----------------------+----------+-------+
    | 01/01/2025 | 31/01/2025 | 30          | ANO-2008      | ANO-2011    | ANO-2017    | ANO-2018    | ANO-2020    | ANO-2021    | ANO-2022    | 1        | 1        | 1         | 1     |
    +------------+------------+-------------+---------------+-------------+-------------+-------------+-------------+-------------+-------------+----------------------+----------+-------+


    Parameters
    ----------
    filename_config: str
        Caminho para o arquivo de configuração

    sheet_name : Optional[str]
        Nome da aba da planilha excel onde está localizado a configuração para o cenario. Se não fornecido, será lido o conteúdp da primeira aba.

    Returns
    -------
    pd.DataFrame
        DataFrame contendo configurações de previsão para cada dia
    """

    def _add_data_ini(data: dict, tipo_data_ini: bool, sheet_name: str):
        lista_data_ini = []
        data_ini_excel = datetime(1899, 12, 30)

        if tipo_data_ini:
            coluna_data = "A"
        else:
            coluna_data = "B"

        for index in range(2, 1000):
            celula = f"{sheet_name}!{coluna_data}{index}"
            dias_planilha = evaluator.evaluate(celula)

            if isinstance(dias_planilha, Blank):
                break

            lista_data_ini.append(data_ini_excel + timedelta(days=int(dias_planilha)))

        if tipo_data_ini:
            data["Data_Ini"] = lista_data_ini
        else:
            data["Data_Fim"] = lista_data_ini

    def _add_colunas(data: dict, sheet_name: str):
        for coluna in [chr(i) for i in range(ord("C"), ord("Z") + 1)]:
            lista_valores = []
            cabecalho = f"{sheet_name}!{coluna}1"
            valor = evaluator.evaluate(cabecalho)

            if isinstance(valor, Blank):
                break

            data[valor.value] = lista_valores

            for index in range(2, 1000):
                celula = f"{sheet_name}!{coluna}{index}"
                valor = evaluator.evaluate(celula)

                if isinstance(valor, Blank):
                    break

                lista_valores.append(valor.value)

    with tempfile.TemporaryDirectory() as temp_dir:
        logging.info("Dir: %s", temp_dir)

        nome_planilha_config = f"{temp_dir}/config.xlsx"
        nome_json_config = f"{temp_dir}/config.json"

        workbook = load_workbook(
            BytesIO(
                SharepointSiteCopel(SITE_SHAREPOINT).get_file(
                    folder=FOLDER_CONFIG, filename=filename_config
                )
            )
        )

        workbook.save(nome_planilha_config)

        compiler = ModelCompiler()
        new_model = compiler.read_and_parse_archive(
            nome_planilha_config, build_code=False
        )
        new_model.persist_to_json_file(nome_json_config)

        reconstituted_model = Model()
        reconstituted_model.construct_from_json_file(nome_json_config, build_code=True)

        evaluator = Evaluator(reconstituted_model)

        data = {}

        _add_data_ini(data, True, sheet_name)
        _add_data_ini(data, False, sheet_name)
        _add_colunas(data, sheet_name)

        df = pd.DataFrame(data)

        logging.info("DataFrame configuração Prevs: %s", df.to_string())

        return df


def _add_config_dias(
    modelo_prevs: str,
    fatores: dict,
) -> dict:
    """
    Cria uma linha do trecho das configurações para os dias

    Parameters
    ----------
    data : datetime
        Data do dia a ser configurado
    modelo_prevs : str
        Tipo da prevs: AVG, P25 ou P50
    data_rodada : datetime
        Data da última rodada do modelos (Ex: data da última rodada do modelo ECENS45av_precip)
    fatores : dict
        Indica o fator a ser aplicado para cada submercado
    vies : str
        Valores aceitados pela TempoOK: com e sem
    Returns
    -------
    dict
        Dicionário contendo a configuração para o dia especificado
    """
    lista_submercados = ["Sudeste", "Sul", "Nordeste", "Norte"]

    config_prevs_dia = {}
    config_prevs_dia["fields"] = {}

    for submercado in lista_submercados:
        config_prevs_dia["fields"][submercado] = {
            "factor": fatores[submercado],
            "lim": None,
            "model": modelo_prevs,
        }

    return config_prevs_dia


def _criar_config_prevs_por_dia(df_config: pd.DataFrame, tipo_prevs: str) -> list:
    """
    Cria o trecho específico da configuração para os dias a serem previstos

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame com a configuração para a criação do dict a ser usado na API TempoOK
    tipo_prevs : str
        Tipo da prevs: AVG, P25 ou P50
    vies : str
        Valores aceitados pela TempoOK: com e sem

    Returns
    -------
    list
        Trecho de configuração para os dias a serem previstos
    """
    lista_config_dia = []

    for _, linha in df_config.iterrows():
        for _ in pd.date_range(linha["Data_Ini"], linha["Data_Fim"]):
            modelo_prevs = linha[tipo_prevs]

            lista_config_dia.append(
                _add_config_dias(
                    modelo_prevs=modelo_prevs,
                    fatores={
                        "Sudeste": linha["FATOR_SE"],
                        "Sul": linha["FATOR_S"],
                        "Nordeste": linha["FATOR_NE"],
                        "Norte": linha["FATOR_N"],
                    },
                )
            )

    return lista_config_dia


def _baixar_vazpast(path_estudo: str, dados_cenario: dict) -> str:
    path_vazpast = f"{path_estudo}vazpast_{dados_cenario['name']}.tar.gz"

    if not os.path.isfile(path_vazpast):
        if "prevs_mensal_url" in dados_cenario:
            url = dados_cenario["prevs_mensal_url"]
            logging.info("Tentando obter URL do vazpast: %s", dados_cenario["name"])
        else:
            raise ValueError("Cenario prevs_mensal_url ausente")

        urllib.request.urlretrieve(url, path_vazpast)

    return path_vazpast


def _criar_df_vazpast(file_path_vazpast_tar_gz: str) -> pd.DataFrame:
    nome_file_vazpast = f"VAZPAST/VAZPAST_{datetime.now().month + 1}_{datetime.now().year}.DAT"

    with tarfile.open(file_path_vazpast_tar_gz, "r:gz") as tar:
        file = tar.extractfile(tar.getmember(nome_file_vazpast))

        if file:
            # Larguras das colunas observadas na pré-visualização dos dados:
            # ID da usina, nome da usina e 12 valores mensais
            col_widths = [5, 14] + [10] * 12

            df = pd.read_fwf(file, widths=col_widths, header=None, skiprows=3)
            df.columns = ["ID_POSTO", "VAZIO"] + ["MES_" + str(i) for i in range(1, 13)]

            return df.drop(columns="VAZIO").set_index("ID_POSTO")

        else:
            raise ValueError("Arquivo tar não possui %s", nome_file_vazpast)


def _criar_df_prevs(df_vazpast: pd.DataFrame) -> pd.DataFrame:
    mes_corrente = datetime.now().month
    valor_mes_corrente = df_vazpast.loc[:, f"MES_{mes_corrente}"].values

    df_prevs = pd.DataFrame(
        {
            "ID_POSTO": df_vazpast.index,
            **{f"PREVISAO_{i}": valor_mes_corrente for i in range(1, 7)},
        }
    )

    df_prevs.index = range(1, len(df_prevs) + 1)

    return df_prevs.astype(int)


def _escrever_df_prevs_arquivo(df: pd.DataFrame, path_estudo: str):
    df = df.reset_index()
    tamanho_colunas = [6, 4] + [9] * 6

    # Ajustar o tamanho de cada coluna do dataframe
    for col, width in zip(df.columns, tamanho_colunas):
        df[col] = df[col].astype(str).apply(lambda x: x.rjust(width))

    path_novo_prevs = f"{path_estudo}{datetime.now().strftime('%Y%m%d')}_prevs_{datetime.now().strftime('%Y_%m')}.rv0"
    with open(path_novo_prevs, "w") as file:
        file.write(df.to_string(header=False, index=False))

    return path_novo_prevs


def criar_prevs_mes_corrente_vazpast(path_estudo: str, dados_cenario: dict) -> str:
    """
    Cria o arquivo de prevs para o mês corrente baseado no vazpast
    disponibilizado pela ferramente de criação de cenários da TempoOK

    Parameters
    ----------
    path_prevs : str
        Localização onde o arquivo de prevs será gerado

    dados_cenario : dict
        Dicionário contendo informações do cenário, como seu nome e afins
    """

    df_vazpast = _criar_df_vazpast(_baixar_vazpast(path_estudo, dados_cenario))
    df_prevs = _criar_df_prevs(df_vazpast)

    return _escrever_df_prevs_arquivo(df=df_prevs, path_estudo=path_estudo)
