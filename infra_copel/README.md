# **infra_copel**

## O que é?

Biblioteca em python para facilitar a interação com a infraestrutura da SCE/DPPC.

Com ela interações com o MongoDB e Sharepoint são simplificadas, além de ficar
transparente seus diferentes locais de uso:
- ambiente local (através de config.ini);
- AirFlow de homologação (com suas Variables e Connections);
- Airflow de produção (com suas Variables e Connections).

## Como instalar

### Modo editável local/desenvolvimento

**No diretório onde foi clonado**, executar:
```
pip install -e .
```
Assim, o caminho local ficará no PATH do python.

#### Arquivo de configuração
**IMPORTANTE**
Copiar o arquivo config_template.ini como config.ini, na mesma pasta.
Substituir as informações necessárias.
```
[Variables]
sharepoint_site = https://4c80k2.sharepoint.com/
sharepoint_user = pedro@4c80k2.onmicrosoft.com
sharepoint_pass = <preencher_aqui>

[historico_oficial]
host = nwautomhml
schema = historico_oficial
login = airflow
password = <preencher_aqui>
```


## Modo em servidor de produção

Caso haja modificações (melhorias ou correção de bugs), lembrar de realizar também o commit da nova tag:
```
# Exemplo
git tag -a v0.1.1 -m "Versão 0.1.1"
git push --tags
```

Criar o arquivo instalador no seu ambiente:
```
python setup.py bdist_wheel
```

O arquivo instalador ficará em:
```
dist\infra_copel-<versao>.whl
```
Copiar o arquivo instalador gerado para o servidor; <br>
Instalá-lo no servidor desejado utilizando o comando:
```
# Lembrar de estar dentro do ambiente virtual caso necessário
pip install infra_copel-<versao>.whl
```

## Uso básico

### MongoDB (database 'historico_oficial')

Como é uma conexão direta a uma database, algumas consultas já estão
programadas com acesso direto a um dataframe formatado.


```python
>>> from infra_copel import MongoHistoricoOficial

>>> mdb = MongoHistoricoOficial()

>>> mdb.df_limites_pld_tarifas

         PLD_MIN  PLD_MAX_EST  PLD_MAX_H  ...    TEO  TEOItaipu   TSA
periodo                                   ...
2017       33.68       533.82        NaN  ...  11.58      33.06   NaN
2018       40.16       505.18        NaN  ...  11.88      31.92  6.88
2019       42.35       513.89        NaN  ...  12.41      35.97   NaN
2020       39.68       559.75    1148.36  ...  12.77      39.68  7.40
2021       49.77       583.88    1197.87  ...  12.74      49.77  7.63
2022       55.70       646.58    1326.50  ...  14.04      55.70  8.41
2023       69.04       684.73    1404.77  ...  15.05      69.04  9.02

>>> mdb.df_ena_mlt
            SE      S     NE      N
periodo
2020-01  65938   7578  13660  15695
2020-02  70764   8354  14372  22782
2020-03  68951   7093  14224  26715
(...)

```

### Sharepoint
```python
>>> df = mdb.df_ena_mlt

>>> from infra_copel import SharepointSiteCopel

>>> sp = SharepointSiteCopel('PowerBIInsightsComercializacao')

>>> sp.write_df_to_excel(df, 'Base de Dados/Teste-readme/ENA_MLT', 'ENA_MLT.xlsx', index=True)

```
