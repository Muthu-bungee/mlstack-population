from suggestion import Suggestion_Provider
from product import Product_Data_Provider
from match import MatchProvider
from mlconfig import MlConfig

conf=MlConfig()
source_store=conf.getValue('main','source_store')
store=conf.getValue('main','store')
score=conf.getValue('main','score')

#Get MAtch suggestion provided by ML stack
sp=Suggestion_Provider()
suggestions_df=sp.get_suggestions(source_store,score)
print('Total suggestions are ', suggestions_df.shape)
unique_suggested_products=sp.get_unique_suggested_products()
print('total unique products are ',len(unique_suggested_products))

#Get latest product attributes to suggested products
pdp=Product_Data_Provider()
product_data_dict=pdp.get_product_data_dict(unique_suggested_products)

# Get customer matches from match library
mp=MatchProvider(store)
active_matches=mp.get_active_matches()
print(active_matches.shape)
deleted_matches=mp.get_deleted_matches()
print(deleted_matches.shape)
