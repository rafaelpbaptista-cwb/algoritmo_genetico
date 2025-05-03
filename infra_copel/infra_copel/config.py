# -*- coding: utf-8 -*-
"""Leitura do arquivo de configuração (se houver)."""
import configparser
import pathlib

# Leitura do arquivo de configuração
CONFIG_FILE = pathlib.Path(__file__).parent.with_name("config.ini")
CONFIG = configparser.ConfigParser()


def get_variable(nome_variavel: str):
    """
    Obtem o valor de uma determinada variável

    Parameters
    ----------
    nome_variavel : str
        Nome da variável que desejamos obter
    """
    CONFIG.read(CONFIG_FILE)

    try:
        # Tenta pegar os dados do arquivo de configuração
        cfg = CONFIG['Variables']
    except KeyError:
        from airflow.models import Variable

        return Variable.get(nome_variavel)
    else:
        # Retorna os dados lidos do arquivo de configuração
        try:
            return cfg[nome_variavel]
        except KeyError:
            raise KeyError(f'Favor inserir no seu arquivo de conf.ini do projeto infra_copel a variável {nome_variavel}')

def cfg_sharepoint() -> dict:
    """
    Dados de conexão com o Sharepoint.

    Returns
    -------
    dict

    """
    # Leitura do arquivo (novamente para garantir novas mudanças)
    CONFIG.read(CONFIG_FILE)

    try:
        # Tenta pegar os dados do arquivo de configuração
        cfg = CONFIG['Variables']
    except KeyError:
        # Tenta pegar os dados do Airflow
        from airflow.models import Variable
        return {'share_point_site': Variable.get('sharepoint_site'),
                'username': Variable.get('sharepoint_user'),
                'password': Variable.get('sharepoint_pass'),
                'verify_ssl': True,
                }
    else:
        # Retorna os dados lidos do arquivo de configuração
        return {'share_point_site': cfg['sharepoint_site'],
                'username': cfg['sharepoint_user'],
                'password': cfg['sharepoint_pass'],
                'verify_ssl': False,
                }


def cfg_mongodb(db_name: str) -> dict[str, str, str, str]:
    """
    Dados de conexão do MongoDB para o database informado.

    Parameters
    ----------
    db_name : str
        Setor do arquivo de configuração a ser consultado.

    Returns
    -------
    dict[str, str, str, str]

    """
    # Leitura do arquivo (novamente para garantir novas mudanças)
    CONFIG.read(CONFIG_FILE)
    try:
        # Tenta pegar os dados do arquivo de configuração
        cfg = CONFIG[db_name]
    except KeyError:
        # Retorna None para que o chamador faça o tratamento
        return None
    else:
        # Retorna os dados lidos do arquivo de configuração
        return {'host': cfg['host'],
                'username': cfg['login'],
                'password': cfg.get('password', raw=True),
                'serverSelectionTimeoutMS': cfg['serverSelectionTimeoutMS']
                }


def cfg_ftp(conn_id: str) -> dict[str, str]:
    """
    Dados de conexão do FTP para o servidor desejado.

    Parameters
    ----------
    conn_id : str
        Identificador da conexão FTP.

    Returns
    -------
    dict[str, str]

    """
    # Leitura do arquivo (novamente para garantir novas mudanças)
    CONFIG.read(CONFIG_FILE)
    try:
        # Tenta pegar os dados do arquivo de configuração
        cfg = CONFIG[conn_id]
    except KeyError:
        # Tenta pegar os dados do Airflow
        from airflow.providers.ftp.hooks.ftp import FTPHook

        conn_ftp = FTPHook.get_connection(conn_id)
        return {'host': conn_ftp.host,
                'user': conn_ftp.login,
                'passwd': conn_ftp._password,
                'extra': conn_ftp.extra_dejson,
                }
    else:
        # Retorna os dados lidos do arquivo de configuração
        extra = {k: cfg.get(k, raw=True)
                 for k in cfg
                 if k not in ('host', 'login', 'password')}
        return {'host': cfg['host'],
                'user': cfg['login'],
                'passwd': cfg.get('password', raw=True),
                'extra': extra,
                }


def cfg_dcide_api() -> dict[str, str]:
    """
    Dados de conexão com a API da DCIDE.

    Returns
    -------
    dict[str, str]

    """
    # Leitura do arquivo (novamente para garantir novas mudanças)
    CONFIG.read(CONFIG_FILE)
    try:
        # Tenta pegar os dados do arquivo de configuração
        cfg = CONFIG['dcide_api']
    except KeyError:
        # Tenta pegar os dados do Airflow
        from airflow.hooks.base import BaseHook

        conn = BaseHook.get_connection('dcide_api')
        return {'userName': conn.login,
                'senha': conn.password,
                'verify_ssl': True,
                }
    else:
        # Retorna os dados lidos do arquivo de configuração
        return {'userName': cfg['login'],
                'senha': cfg.get('password', raw=True),
                'verify_ssl': False,
                }
    
def cfg_rolling_api() -> tuple:
    """
    Dados para a autenticação para o uso da API desenvolvida para o Rolling/Vaz20

    Returns
    -------
    tuple
        Usuário e senha para autenticação para o uso da API
    """    
    CONFIG.read(CONFIG_FILE)
    try:
        # Tenta pegar os dados do arquivo de configuração
        cfg = CONFIG['rolling_api']
    except KeyError:
        # Tenta pegar os dados do Airflow
        from airflow.hooks.base import BaseHook

        conn = BaseHook.get_connection('rolling_api')
        return (conn.login, conn.password)
    else:
        # Retorna os dados lidos do arquivo de configuração
        return (cfg['login'], cfg.get('password', raw=True))
    
def cfg_site_sintegre() -> tuple:
    """
    Dados para a autenticação para o uso do site do sintegre via selenium

    Returns
    -------
    tuple
        Usuário e senha para autenticação para o uso do site do selenium 
    """    
    CONFIG.read(CONFIG_FILE)
    try:
        # Tenta pegar os dados do arquivo de configuração
        cfg = CONFIG['site_sintegre']
    except KeyError:
        # Tenta pegar os dados do Airflow
        from airflow.hooks.base import BaseHook

        conn = BaseHook.get_connection('site_sintegre')
        return (conn.login, conn.password)
    else:
        # Retorna os dados lidos do arquivo de configuração
        return (cfg['login'], cfg.get('password', raw=True))