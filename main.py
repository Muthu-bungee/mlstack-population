from suggestion import Suggestion_Provider
from product import Product_Data_Provider
from match import MatchProvider
from mlconfig import MlConfig

conf=MlConfig()
source_store=conf.getValue('main','source_store')
score=conf.getValue('main','score')

#Get MAtch suggestion provided by ML stack
suggestion_provider=Suggestion_Provider()
suggestions_df=suggestion_provider.get_suggestions(source_store,score)
print('Total suggestions are ', suggestions_df.shape)
unique_suggested_products=suggestion_provider.get_unique_suggested_products()
print('total unique products are ',len(unique_suggested_products))

#Get latest product attributes to suggested products
product_data_provider=Product_Data_Provider()
product_data_dict=product_data_provider.get_product_data_dict(unique_suggested_products)

# Get customer matches from match library
match_provider=MatchProvider()
match_provider.init_customer_matches()
active_matches=match_provider.get_active_matches()
print(active_matches.shape)
deleted_matches=match_provider.get_deleted_matches()
print(deleted_matches.shape)
not_matches=match_provider.get_not_matches()
print(not_matches.shape)