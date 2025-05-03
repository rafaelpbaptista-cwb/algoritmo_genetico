"""
Tratamento temporário das prevs criadas pela TempoK
"""

import zipfile
import os
import logging
import tempfile
import shutil
from glob import glob
import pandas as pd
from infra_copel import SharepointSiteCopel


SITE_SHAREPOINT = "PowerBIInsightsComercializacao"
FOLDER_CONFIG_PREVS_TEMPOOK = "Base de Dados/Configuracao Cenarios Prevs TempoOK"
FILE_AJUSTES_PREVS = "ajustes_prevs_postos.xlsx"


def _ler_config() -> pd.DataFrame:
    return SharepointSiteCopel(SITE_SHAREPOINT).read_df_from_excel(
        folder=FOLDER_CONFIG_PREVS_TEMPOOK, filename=FILE_AJUSTES_PREVS
    )


def _descompactar_prevs_zip(dir_prevs_zip: str, dir_descompactar: str):
    for prevs_zip in glob(f"{dir_prevs_zip}/ANO_*.zip"):
        nome_sub_dir = prevs_zip.split(os.sep)[-1].replace(".zip", "")
        dir_descompactar_prevs = f"{dir_descompactar}/{nome_sub_dir}"

        os.mkdir(dir_descompactar_prevs)

        with zipfile.ZipFile(prevs_zip, "r") as zip_ref:
            logging.info(
                "Descompactando zip de prevs: %s no diretório %s",
                prevs_zip,
                dir_descompactar,
            )
            zip_ref.extractall(dir_descompactar_prevs)


def _substituir_vazao_postos_arquivo_rv(df_config: pd.DataFrame, file_prevs: str):
    logging.info("Substituindo vazao de postos no arquivo: %s", file_prevs)

    col_widths = [6, 5] + [10] * 6
    df = pd.read_fwf(file_prevs, widths=col_widths, header=None)

    for _, row in df_config.iterrows():
        df.iloc[df[1] == row["id_posto"], 2:] = row[2:]

    _salvar_prevs_alterado(df, file_prevs)


def _salvar_prevs_alterado(df: pd.DataFrame, file_prevs: str):
    tamanho_colunas = [6, 4] + [9] * 6

    # Ajustar o tamanho de cada coluna do dataframe
    for col, width in zip(df.columns, tamanho_colunas):
        df[col] = df[col].astype(str).apply(lambda x: x.rjust(width))

    with open(file_prevs, "w") as file:
        file.write(df.to_string(header=False, index=False))


def _substituir_vazao_postos(df_config: pd.DataFrame, dir_descompactar: str):
    for dir_prevs_descompactado in os.listdir(dir_descompactar):
        for tratamento_mes in list(df_config.mes_prevs.unique()):
            for file_prevs in glob(
                f"{dir_descompactar}/{dir_prevs_descompactado}/*{tratamento_mes.strftime('%Y_%m')}*.rv*"
            ):
                _substituir_vazao_postos_arquivo_rv(
                    df_config[
                        df_config.mes_prevs.dt.strftime("%Y_%m")
                        == tratamento_mes.strftime("%Y_%m")
                    ],
                    file_prevs,
                )


def _criar_backup_original_prevs_zip(dir_estudos: str):
    path_file = f"{dir}backup"

    if not os.path.exists(path_file):
        try:
            os.mkdir(path_file)
        except:
            logging.debug("Não conseguiu criar diretório de backup.")

    for original_prevs_zip in glob(f"{dir_estudos}/ANO_*.zip"):
        shutil.move(original_prevs_zip, f"{dir_estudos}/backup")


def _criar_novo_prevs_zip(dir: str, temp_dir: str):
    for nome_zip in os.listdir(temp_dir):
        with zipfile.ZipFile(
            f"{dir}/{nome_zip}.zip", "w", zipfile.ZIP_DEFLATED
        ) as zipf:
            for root, _, files in os.walk(f"{temp_dir}/{nome_zip}"):
                for file in files:
                    file_path = os.path.join(root, file)
                    zipf.write(file_path, file)


def substuir_vazao_postos(dir_estudos: str) -> None:
    """
    Substitui a vazão dos postos nos arquivos de prevs a serwm utilizados nas rodadas do rolling.

    Essa função realiza as seguintes etapas:
    1. Lê o arquivo de configuração usando a função `_ler_config()`.
    2. Cria um diretório temporário usando `tempfile.TemporaryDirectory()`.
    3. Descompacta os arquivos de dados zip na pasta temporária usando `_descompactar_prevs_zip()`.
    4. Substitui a vazão de postos nos arquivos de dados usando `_substituir_vazao_postos()`.
    5. Cria um backup dos arquivos de dados originais zip usando `_criar_backup_original_prevs_zip()`.
    6. Cria um novo arquivo zip com os arquivos de dados processados usando `_criar_novo_prevs_zip()`

    Parameters
    ----------
    dir : str
        O diretório contendo os arquivos de dados a serem processados.
    """

    if FILE_AJUSTES_PREVS in SharepointSiteCopel(SITE_SHAREPOINT).list_filenames(
        FOLDER_CONFIG_PREVS_TEMPOOK
    ):
        df_config = _ler_config()

        with tempfile.TemporaryDirectory() as temp_dir:
            _descompactar_prevs_zip(dir_estudos, temp_dir)
            _substituir_vazao_postos(df_config, temp_dir)
            _criar_backup_original_prevs_zip(dir_estudos)
            _criar_novo_prevs_zip(dir_estudos, temp_dir)
