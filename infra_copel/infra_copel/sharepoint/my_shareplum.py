# -*- coding: utf-8 -*-
"""Funções para facilitar o uso do shareplum."""
import requests

from infra_copel.sharepoint.shareplum import Office365
from infra_copel.sharepoint.shareplum import Site
from infra_copel.sharepoint.shareplum.site import Version


def get_sharepoint_site(share_point_site: str,
                        username: str,
                        password: str,
                        site_url: str,
                        verify_ssl: bool = True,
                        ) -> Site:
    """
    Retorna um objeto referente ao site do sharepoint.

    Parameters
    ----------
    share_point_site : str
        Site do sharepoint ("https://<something>.sharepoint.com/)
    username : str
        Usuário do sharepoint.
    password : str
        Senha do usuário do sharepoint.
    site_url : str
        URL completa do site
        ("https://<something>.sharepoint.com/sites/something).
    verify_ssl : bool, default is True
        Verificar SSL. Desabilitar se houver problemas com certificados.

    Returns
    -------
    Site
        Objeto que representa um site do sharepoint.

    """
    if verify_ssl is False:
        # Desabilita warnings
        requests.packages.urllib3.disable_warnings(
            requests.packages.urllib3.exceptions.InsecureRequestWarning
            )

    # Conexão com o sharepoint
    authcookie = Office365(share_point_site=share_point_site,
                           username=username,
                           password=password).GetCookies()

    # Site desejado
    return Site(site_url=site_url,
                version=Version.v2016,
                authcookie=authcookie,
                verify_ssl=verify_ssl)
