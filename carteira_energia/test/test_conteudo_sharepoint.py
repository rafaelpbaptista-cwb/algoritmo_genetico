from carteira_energia.entidades.algoritmo_genetico import AlgoritmoGenetico
from carteira_energia.sharepoint.gerenciador_dados_sharepoint import GerenciadorArquivosPLDSharepoint, GerenciadorArquivosSharepointPortifolioRecomendacao

def test_dowload_arquivo_media_previsao_pld():
    qtdade_meses_ano = 12

    gerenciador_sharepoint = GerenciadorArquivosPLDSharepoint()
    lista_valores_pld = gerenciador_sharepoint.get_dataframe_media_meses_ano()
    
    assert len(lista_valores_pld) == qtdade_meses_ano, f'Deve ser retornado uma lista de tamanho {qtdade_meses_ano} e não de {len(lista_valores_pld)}'

def test_upload_arquivo_sugestao_carteira(algoritmo_genetico: AlgoritmoGenetico):
    qtdade_individuos_exportados = 3
    gerenciador_sharepoint = GerenciadorArquivosSharepointPortifolioRecomendacao()

    gerenciador_sharepoint.exportar_resultado_carteira_recomendacao(lista_individuos_exportacao=algoritmo_genetico.lista_populacao[0:qtdade_individuos_exportados])