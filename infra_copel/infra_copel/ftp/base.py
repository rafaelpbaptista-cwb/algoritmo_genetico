# -*- coding: utf-8 -*-
"""Classe base para conexões FTP."""
from __future__ import annotations

from ftplib import FTP
from io import BytesIO
from pathlib import Path
import re

import pandas as pd

from infra_copel.config import cfg_ftp

# Regex para salvar o diretório de mudança
RE_CWD = re.compile('250 Directory changed to (?P<directory>.+)')


class FTPBase:
    """Classe base para conexões FTP."""

    def __init__(self, ftp_conn_name: str):

        self._ftp_conn_name = ftp_conn_name

        self._host = None
        self._user = None
        self._passwd = None
        self._extra = None

        self._last_pwd: None = None

        self._ftp: FTP | None = None

    @property
    def ftp(self):
        """Conexão ftp."""
        if self._ftp is None:
            self.connect()
        # Muda para o último diretório ativo em caso de reconexão
        if self._last_pwd:
            self._ftp.cwd(self._last_pwd)
        return self._ftp

    def _get_config(self):

        cfg = cfg_ftp(self._ftp_conn_name)

        self._host = cfg['host']
        self._user = cfg['user']
        self._passwd = cfg['passwd']
        self._extra = cfg['extra']

    def connect(self):
        """Realiza uma nova conexão FTP."""
        if self._host is None:
            self._get_config()
        self._ftp = FTP(self._host, self._user, self._passwd)

    def cd(self, dirname: str):
        """Muda o diretório."""
        try:
            resp = self.ftp.cwd(dirname)
        except ConnectionError:
            self.connect()
            resp = self.ftp.cwd(dirname)
        if match := RE_CWD.match(resp):
            # Salva diretório atual caso necessário
            self._last_pwd = match.group('directory')
        return resp

    def ls(self, path: str = '') -> pd.DataFrame:
        """
        Retorna o conteúdo do diretório em um dataframe.

        Parameters
        ----------
        path : str, optional
            Diretório a ser mostrado o conteúdo.
            Se não informado mostra o conteúdo do diretório atual.

        Returns
        -------
        DataFrame
            Informações dos arquivos do diretório.

        """
        rows = []
        try:
            gen_mlsd = self.ftp.mlsd(path)
        except ConnectionError:
            self.connect()
            gen_mlsd = self.ftp.mlsd(path)
        for filename, facts in gen_mlsd:
            row = {'filename': filename,
                   'size': int(facts['size']),
                   'modify': pd.to_datetime(facts['modify'],
                                            format='%Y%m%d%H%M%S.%f'),
                   'type': facts['type'],
                   }
            rows.append(row)
        df_ls = pd.DataFrame(rows)

        return df_ls

    def pwd(self) -> str:
        """Retorna o diretório corrente."""
        try:
            return self.ftp.pwd()
        except ConnectionError:
            self.connect()
            return self.ftp.pwd()

    def get_bytes_io(self, filepath):
        """Realiza o download do conteúdo do arquivo binário."""
        filepath = Path(filepath)
        bytes_io = BytesIO()
        try:
            self.ftp.retrbinary(f'RETR {filepath}',
                                bytes_io.write)
        except ConnectionError:
            self.connect()
            self.ftp.retrbinary(f'RETR {filepath}',
                                bytes_io.write)
        bytes_io.seek(0)
        return bytes_io

    def read_csv(self, filepath, **kwargs):
        """Leitura de um csv do servidor FTP."""
        return pd.read_csv(self.get_bytes_io(filepath), **kwargs)
