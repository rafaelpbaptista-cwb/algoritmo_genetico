import copy
import logging
import csv
import locale
from random import random, choice, seed
from operator import add, sub
from pandas import DataFrame
import matplotlib.pyplot as plt
from carteira_energia.entidades.configuracao_cenario import ConfiguracaoCenario
from carteira_energia.entidades.individuo import Individuo
from carteira_energia.util.utilidades import get_maior_nota_avaliacao_disponivel

locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8') 

class AlgoritmoGenetico():
    """
    Classe que utiliza algoritmo genético para a solução do problema de sugestão de carteira de compra e venda de energia
    """

    def __init__(self, ano_simulacao: int) -> None:
        logging.info("Iniciando algoritmo genético para o ano %s", ano_simulacao)

        seed(146)

        self.configuracao_cenario = ConfiguracaoCenario(ano_simulacao)
        
        self.lista_populacao = [Individuo(self.configuracao_cenario) for _ in range(self.configuracao_cenario.tamanho_populacao)]
        logging.info("Gerando população inicial. Quantidade de individuos: %s", len(self.lista_populacao))

        self._lista_melhores_individuos = []
        
        self.melhor_individuo = None
    
    def get_lista_melhores_individuos(self, sem_duplicidade=True):
        self._lista_melhores_individuos.sort(reverse=True)

        if sem_duplicidade:
            lista_sem_duplicidade = []

            for individuo in self._lista_melhores_individuos:
                if individuo not in lista_sem_duplicidade:
                    lista_sem_duplicidade.append(individuo)

            return lista_sem_duplicidade

        return self._lista_melhores_individuos

    def _avaliar_populacao(self):
        self.lista_populacao = [individuo for individuo in self.lista_populacao if individuo.ganhos_financeiro_melhor_cenario > 0]
        self.lista_populacao.sort(reverse=True)

    def _selecionar_melhores_individuos(self):
        if self.lista_populacao:
            melhor_individuo_rodada = self.lista_populacao[0]
            self._lista_melhores_individuos.append(melhor_individuo_rodada)

            if self.melhor_individuo is None or self.melhor_individuo < melhor_individuo_rodada:

                self.melhor_individuo = melhor_individuo_rodada

    def _selecao_pais(self):
        soma_pontuacao = sum(individuo.nota_avaliacao for individuo in self.lista_populacao)

        pai = self._sortear_individuo(valor_sorteado=random() * soma_pontuacao)
        mae = self._sortear_individuo(valor_sorteado=random() * soma_pontuacao)

        return pai, mae

    def _sortear_individuo(self, valor_sorteado):
        indice_selecionado = 0
        aux_andamento_sorteio = 0

        for indice, individuo in enumerate(self.lista_populacao):
            logging.debug('Nota do individuo: %s', individuo.nota_avaliacao)
            logging.debug('aux_andamento_sorteio: %s', aux_andamento_sorteio)

            if aux_andamento_sorteio < valor_sorteado:
                aux_andamento_sorteio +=  individuo.nota_avaliacao
                indice_selecionado = indice
            else:
                break
        
        # Logica para priorizar os primeiros individuos da lista pois são esses que possuem o melhor resultado esperado (menor pontuação)
        # Queremos obter o menor valor possível
        indice_selecionado = (len(self.lista_populacao) - 1) - indice_selecionado
        individuo_sorteado = self.lista_populacao[indice_selecionado]

        logging.debug('Indice individuo selecionado: %s', indice_selecionado)

        return individuo_sorteado



    def _crossover(self):
        qtdade_meses_ano = 12
        lista_individuo_nova_geracao = []

        # Como é adicionado 2 filhos por rodada, o laço de repetição tem que ir até a metade do tamanho da população
        for indice in range(round(self.configuracao_cenario.tamanho_populacao / 2)):
            pai, mae = self._selecao_pais()

            area_corte_cromosso_individuo = round(random() * qtdade_meses_ano)

            filho1 = Individuo(configuracao_cenario=self.configuracao_cenario, gerar_cromossomo=False, geracao=pai.geracao+1)
            filho2 = Individuo(configuracao_cenario=self.configuracao_cenario, gerar_cromossomo=False, geracao=pai.geracao+1)

            filho1.lista_cromossomo = copy.deepcopy(pai.lista_cromossomo[0:area_corte_cromosso_individuo] + mae.lista_cromossomo[area_corte_cromosso_individuo::])
            filho2.lista_cromossomo = copy.deepcopy(pai.lista_cromossomo[0:area_corte_cromosso_individuo] + mae.lista_cromossomo[area_corte_cromosso_individuo::])

            lista_individuo_nova_geracao.append(filho1)
            lista_individuo_nova_geracao.append(filho2)
        
        self.lista_populacao = lista_individuo_nova_geracao
        self._avaliar_populacao()

    def _mutacao_gene(self):
        for individuo in self.lista_populacao:
            if random() < self.configuracao_cenario.taxa_mutacao:
                posicao_gene_mutacao = round(random() * (len(individuo.lista_cromossomo) - 1))

                gene_mutacao = individuo.lista_cromossomo[posicao_gene_mutacao]
                valor_mutacao = round(gene_mutacao.qtdade_energia_mwm_venda * random())

                valor_antigo_gene = gene_mutacao.qtdade_energia_mwm_venda

                operacao_matematica_randomica = choice((add, sub))
                gene_mutacao.qtdade_energia_mwm_venda = operacao_matematica_randomica(gene_mutacao.qtdade_energia_mwm_venda, valor_mutacao)

                logging.debug('Gene %s do individuo %s sofreu mutacao', individuo, gene_mutacao)
                logging.debug('Qtdade de energia era %s e foi para %s', valor_antigo_gene, gene_mutacao.qtdade_energia_mwm_venda)

    def _ajustar_individuos(self):
        lista_individuos_ajuste = [individuo for individuo in self.lista_populacao if individuo.nota_avaliacao == get_maior_nota_avaliacao_disponivel(self.configuracao_cenario.tamanho_populacao)]

        probabilidade_ajuste_individuo = 0.8

        for individuo in lista_individuos_ajuste:
            numero_sorteado = random()
            
            if numero_sorteado < probabilidade_ajuste_individuo:
                individuo.ajustar_cromossomo()

    def executar(self):
        geracao = 0
        
        while True:
            logging.info('Executando a rodada/geracao %s', geracao)

            logging.debug("_ajustar_individuos")
            self._ajustar_individuos()

            logging.debug("_avaliar_populacao")  
            self._avaliar_populacao()

            logging.debug("_selecionar_melhores_individuos")
            self._selecionar_melhores_individuos()

            logging.debug("_crossover")
            self._crossover()

            logging.debug("_mutacao_gene")
            self._mutacao_gene()

            if self.melhor_individuo.geracao + self.configuracao_cenario.limite_qtdade_geracoes_melhor_individuo == geracao:
                logging.info('O melhor individuo esta a %s rodadas no top da selecao. Resolucao proplema finalizado', self.configuracao_cenario.limite_qtdade_geracoes_melhor_individuo)
                break

            logging.info('Geracao melhor individuo: %s. Nota: %s. Ganhos R$: %s. Risco: %s',
                            self.melhor_individuo.geracao,
                            locale.format_string('%.2f',self.melhor_individuo.nota_avaliacao, grouping=True),
                            locale.format_string('%.2f', self.melhor_individuo.ganhos_financeiro_melhor_cenario, grouping=True),
                            locale.format_string('%.2f', self.melhor_individuo.risco_financeiro_cenario, grouping=True))
            
            geracao += 1

    def plotar_imagem_evolucao_melhores_individuos(self):
        lista_melhores_individuos = self.get_lista_melhores_individuos(sem_duplicidade=False)
        lista_melhores_individuos.sort(key=lambda gene: gene.geracao)
        lista_melhores_notas = [individuo.nota_avaliacao for individuo in lista_melhores_individuos]

        plt.plot(lista_melhores_notas)
        
        plt.title("Acompanhamento das melhores notas")
        plt.xlabel('Geração')
        plt.ylabel('Pontuação')

        plt.ylim(0)
        current_values = plt.gca().get_yticks()
        plt.gca().set_yticklabels(['{:,.0f}'.format(x) for x in current_values])

        plt.savefig(f'acompanhamento_melhores_notas_ano_{self.configuracao_cenario.ano_simulacao}.png', bbox_inches='tight')
        plt.close()

    def gerar_csv_resposta(self):
        nome_csv_exportado = f'dados_mes_ano{self.configuracao_cenario.ano_simulacao}.csv'
        lista_venda_energia_mes = []

        for melhor_individuo in self.get_lista_melhores_individuos():
            melhor_individuo.lista_cromossomo.sort(key=lambda gene: gene.sequencia_mes)

            lista_valores = [gene.qtdade_energia_mwm_venda for gene in melhor_individuo.lista_cromossomo]
            self._preencher_valores_12_meses(lista_valores)

            lista_valores.append(sum(lista_valores))
            lista_valores.append(melhor_individuo.ganhos_financeiro_melhor_cenario)
            lista_valores.append(melhor_individuo.risco_financeiro_cenario)
            lista_valores.append(melhor_individuo.geracao)
            lista_valores.append(melhor_individuo.nota_avaliacao)

            lista_venda_energia_mes.append(lista_valores)

        df_valores_venda_mes = \
            DataFrame(columns=['Jan','Fev','Marc','Abr','Maio','Jun','Jul','Ago',
                            'Set','Out','Nov','Dez', 'Total', 'Total R$ melhor cenario',
                            'Total R$ pior cenario', 'Geracao', 'Nota avaliacao'])

        for indice, valor_lista in enumerate(lista_venda_energia_mes):
            df_valores_venda_mes.loc[indice] = valor_lista

        df_valores_venda_mes.to_csv(nome_csv_exportado, sep=';', decimal=',', index=False)

        # Acrescentar informações do cenário que resultou o resultado
        with open(nome_csv_exportado, 'a') as file_csv_exportado:
            file_csv_exportado.write('\n\n')
            file_csv_exportado.write(f'Meta (MWmédio);{self.configuracao_cenario.meta_anual_venda_kwm}\n')
            file_csv_exportado.write(f'VE Fat;{self.configuracao_cenario.volume_financeiro_meta_ganhos}\n')
            file_csv_exportado.write(f'RISK A;{self.configuracao_cenario.volume_financeiro_risco_anual}\n')

            file_csv_exportado.write('\n\n')
            file_csv_exportado.write('Preço PLD Mês\n')
            file_csv_exportado.write('Jan;Fev;Marc;Abr;Maio;Jun;Jul;Ago;Set;Out;Nov;Dez\n')

            writer_cvs = csv.writer(file_csv_exportado, delimiter=';', lineterminator='\n')
            writer_cvs.writerow(self.configuracao_cenario.lista_preco_pld_mes)

            file_csv_exportado.write('\n\n')
            file_csv_exportado.write('Preço Risco Mês\n')
            file_csv_exportado.write('Jan;Fev;Marc;Abr;Maio;Jun;Jul;Ago;Set;Out;Nov;Dez\n')

            writer_cvs.writerow(self.configuracao_cenario.lista_risco_mes)

    def _preencher_valores_12_meses(self, lista: list, tamanho_desejavel: int = 12):
        while len(lista) < tamanho_desejavel:
            lista.append(0)