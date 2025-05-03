from carteira_energia.formulas.formulas_agoritmo_genetico import calcular_variavel_prisco
from carteira_energia.sharepoint.gerenciador_dados_sharepoint import GerenciadorArquivosPLDSharepoint

def test_calcular_variavel_prisco():
     sharepoint = GerenciadorArquivosPLDSharepoint()

     dataframe_preco_pld_mes = sharepoint.get_dataframe_media_meses_ano()
     dataframe_risco_mes = sharepoint.get_valores_pld_cenario('P75')

     dataframe_risco_mes = calcular_variavel_prisco(dataframe_preco_pld_medio_mes=dataframe_preco_pld_mes, 
                                                    dataframe_cenarios_pld=dataframe_risco_mes, 
                                                    ano_simulacao=1)

     for indice, valor_risco_mes_calculado in enumerate(dataframe_risco_mes['VALOR_RISCO'].to_list()):
          valor_verificar = dataframe_risco_mes['VALOR_MEDIA_PLD_CENARIOS'][indice] - dataframe_risco_mes['VALOR_PLD_MINIMO'][indice] * (1 - (indice + 1) / 100)

          assert valor_verificar == valor_risco_mes_calculado, f'Valor risco mês errado. Esperado: {valor_verificar}. Valor atual: {valor_risco_mes_calculado}'