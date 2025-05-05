from carteira_energia.carteira_compra_venda_energia import CarteiraCompraVendaEnergia

import logging

logging.basicConfig(
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
    level=logging.INFO,
    handlers=[logging.FileHandler("log_execucao.log"), logging.StreamHandler()],
)

aux = CarteiraCompraVendaEnergia(2023)

aux.encontrar_recomendacao()

aux.exportar_resultado_pasta_sharepoint()
