import datetime
from infra_copel.api.dcide import DcideApi
from infra_copel.mongodb.dcide import MongoDcide

dcide_api = DcideApi()

df = dcide_api.pool_df_data_range(data_inicial=dcide_api.data_mais_antiga.strftime('%d/%m/%Y'), data_final=datetime.datetime.now().strftime('%d/%m/%Y'))

MongoDcide().popular_mongo_dcide(df)