from db.athena import AthenaExecuter
import pandas as pd
from mlconfig import MlConfig


class Suggestion_Provider:
    def __init__(self):
        conf=MlConfig()
        db_name=conf.getValue('main','suggestion_db_name')
        print('dbname in sugg is',db_name)
        self.executor=AthenaExecuter(db_name)
        
    def _get_suggestions_from_Db(self,base_store,score):
        query = "SELECT * FROM embedder_matches_grocery_18may2023 where\
        (base_source_store not in ('<>shoprite','<>cvs') and comp_source_store not in ('<>shoprite','<>cvs')) and\
        (base_source_store = '{0}' or comp_source_store = '{0}')\
        and score >= {1} and base_source_store != comp_source_store  limit 100".format(base_store,score)
        df=self.executor.execute(query)
        print(f'Total match recommendations are {df.shape[0]}')
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