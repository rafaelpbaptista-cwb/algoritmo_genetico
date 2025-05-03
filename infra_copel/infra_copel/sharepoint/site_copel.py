# -*- coding: utf-8 -*-
"""Site do Sharepoint da Copel."""

from infra_copel.config import cfg_sharepoint
from infra_copel.sharepoint.site_base import SharepointSite


class SharepointSiteCopel(SharepointSite):
    """
    Representa um Site no Sharepoint da Copel.

    É feito um apontamento para locais diferentes dependendo da configuração.
    """

    def __init__(self, site):

        # Dados do arquivo de configuração ou do airflow
        cfg = cfg_sharepoint()
        # Para garantir a remoção da / caso exista
        site_url_base = cfg['share_point_site'].removesuffix('/')

        super().__init__(share_point_site=cfg['share_point_site'],
                         username=cfg['username'],
                         password=cfg['password'],
                         site_url=f'{site_url_base}/sites/{site}',
                         verify_ssl=False,
                         )
