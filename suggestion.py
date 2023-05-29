
from db.athena import AthenaExecuter
import pandas as pd
from mlconfig import MlConfig
from tqdm.notebook import tqdm

class Suggestion_Provider:
    def __init__(self):
        conf=MlConfig()
        db_name=conf.getValue('main','suggestion_db_name')
        self.executor=AthenaExecuter(db_name)
        
    def _get_suggestions_from_Db(self,base_store,score):
        query = "SELECT * FROM embedder_matches_grocery_18may2023 where\
        (base_source_store not in ('<>shoprite','<>cvs') and comp_source_store not in ('<>shoprite','<>cvs')) and\
        (base_source_store = '{0}' or comp_source_store = '{0}')\
        and score >= {1} and base_source_store != comp_source_store ".format(base_store,score)
        df=self.executor.execute(query)
        return df
    
    def get_suggestions(self,base_store,score):
        directed_suggestions_df=self._get_suggestions_from_Db(base_store,score)
        undirected_suggestions_df = directed_suggestions_df.copy().rename({'uuid_a':'uuid_b','uuid_b':'uuid_a',
                                                          'base_source_store':'comp_source_store',
                                                          'comp_source_store':'base_source_store'},axis='columns')
        final_suggestions_df = pd.concat([directed_suggestions_df,
                                undirected_suggestions_df])
        final_suggestions_df = final_suggestions_df.drop_duplicates(['uuid_a','uuid_b'])
        self.final_suggestions=final_suggestions_df
        return final_suggestions_df
    
    def get_unique_suggested_products(self):
        unique_products=set(self.final_suggestions.uuid_a.to_list()+self.final_suggestions.uuid_b.to_list())
        self.unique_products=unique_products
        return unique_products

class SuggestionPruner:
    def prune_pairs(self,suggestions_df,unique_suggested_products,active_match_dict,deleted_matches,product_data_dict):
       print('suggestions_df size',suggestions_df.shape[0])
       print('unique_suggested_products size',len(unique_suggested_products))
       print('active_match_dict size',len(active_match_dict))
       print('deleted_matches size',len(deleted_matches))
       print('product_data_dict size',len(product_data_dict))
       
       queue_type='fastlane'
       apply_match_pruning=True
       suggested_match_pairs = list(zip(suggestions_df['uuid_a'],suggestions_df['uuid_b'],suggestions_df['score']))
       print('suggested_match_pairs size',len(suggested_match_pairs))

       manager_queue_pairs = []
       pair_to_score_dict = {}
       missing_matches = []
       missing_pairs= set()

       for ind,data in tqdm(enumerate(suggested_match_pairs)):
            print(ind)
            uuid_a,uuid_b,score = data[0],data[1],data[2]
            if (uuid_a in product_data_dict and uuid_b in product_data_dict):
                sku_uuid_a = '<>'.join(uuid_a.split('<>')[1:])
                sku_uuid_b = '<>'.join(uuid_b.split('<>')[1:])
                store_a = sku_uuid_a.split('<>')[-1]
                store_b = sku_uuid_b.split('<>')[-1]
                
                if apply_match_pruning:
                    if ((sku_uuid_a in active_match_dict and store_b not in active_match_dict[sku_uuid_a]) or\
                        sku_uuid_a not in active_match_dict)  and \
                        ((sku_uuid_b in active_match_dict and store_a not in active_match_dict[sku_uuid_b]) or\
                        sku_uuid_b not in active_match_dict):
                        #print(sku_uuid_a,sku_uuid_b,row.score,store_a,store_b)
                        manager_queue_pairs.append((uuid_a,uuid_b))
                        if queue_type != 'fastlane':
                            pair_to_score_dict[(uuid_a,uuid_b)] = 1
                        else:
                            pair_to_score_dict[(uuid_a,uuid_b)] = score
                        print('added to pair_to_score_dict')
                    else :
                        #pass//match already present in system for this pair ??
                        print('already present in system')
                        print(sku_uuid_a,sku_uuid_b,score)
                else:
                    if queue_type != 'fastlane':
                        pair_to_score_dict[(uuid_a,uuid_b)] = 1
                    else:
                        pair_to_score_dict[(uuid_a,uuid_b)] = score
            else:
                missing_pairs.add((uuid_a,uuid_b))
                print('added to missing pairs')

            if (uuid_b not in product_data_dict):
                missing_matches.append(uuid_b)

            if (uuid_a not in product_data_dict):
                missing_matches.append(uuid_a)
                
            pair_to_score_dict = sorted(pair_to_score_dict.items() ,key=lambda x: x[1],reverse=True)
            print('size of pair_to_score_dict is',len(pair_to_score_dict))
            final_pair_to_score_dict = {}
            for pair,score in pair_to_score_dict:
                sku_uuid_a,sku_uuid_b= '<>'.join(pair[0].split('<>')[1:]),'<>'.join(pair[1].split('<>')[1:])
                #print(su_uuid_a,sku_uuid_b)
                if (sku_uuid_a,sku_uuid_b) not in deleted_matches:
                    final_pair_to_score_dict[pair] = score
                
            return final_pair_to_score_dict
            
        



    
