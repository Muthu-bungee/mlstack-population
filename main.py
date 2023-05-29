from suggestion import Suggestion_Provider
from product import Product_Data_Provider
from suggestion import SuggestionPruner
from match import MatchProvider
from mlconfig import MlConfig
from tqdm.notebook import tqdm

conf=MlConfig()
source_store=conf.getValue('main','source_store')
score=conf.getValue('main','score')

#Get MAtch suggestion provided by ML stack
suggestion_provider=Suggestion_Provider()
print('get suggesstion begins ')
suggestions_df=suggestion_provider.get_suggestions(source_store,score)
print('Total suggestions fetched are ', suggestions_df.shape[0])

unique_suggested_products=suggestion_provider.get_unique_suggested_products()
print('total unique suggested products are ',len(unique_suggested_products))

#Get latest product attributes to suggested products
product_data_provider=Product_Data_Provider()
print('creating product data dictionary from ml input for selected unique products')
product_data_dict=product_data_provider.get_product_data_dict(unique_suggested_products)
print('number of records in product data dict are ',len(product_data_dict))

# Get customer matches from match library
match_provider=MatchProvider()
print('initiating customer match flow')
match_provider.init_customer_matches()

active_match_df=match_provider.get_active_matches()
print('active custer matches are ',active_match_df.shape[0])

deleted_matches=match_provider.get_deleted_matches()
print('total deleted matches are ', len(deleted_matches))

not_match_df=match_provider.get_not_matches()
print('no matcehs count:', not_match_df.shape)

 #y should we add not matches here?
match_and_nomatch_store_dict=match_provider.get_uuid_to_store_dict(active_match_df,not_match_df)
print('match_and_nomatch_store_dict size',len(match_and_nomatch_store_dict))


#y should we pass deleted matches here?
pruner =SuggestionPruner()
pairs=pruner.prune_pairs(suggestions_df,unique_suggested_products,match_and_nomatch_store_dict,deleted_matches,product_data_dict)
print('number of pairs to be populated into queue are ',len(pairs))



