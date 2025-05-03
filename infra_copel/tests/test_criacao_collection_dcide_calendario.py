from infra_copel.mongodb.dcide import MongoDcide

mongo = MongoDcide()

mongo['dcide_calendario'].delete_many({})

for ano in range(2012, 2024):
    mongo.get_dados_insercao_dcice_calendario(ano_base=ano, ano_produdo=ano+1)
    mongo.get_dados_insercao_dcice_calendario(ano_base=ano, ano_produdo=ano+2)
    mongo.get_dados_insercao_dcice_calendario(ano_base=ano, ano_produdo=ano+3)
    mongo.get_dados_insercao_dcice_calendario(ano_base=ano, ano_produdo=ano+4)