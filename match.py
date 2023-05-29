from db.aurora import AuroraExecutor
from db.athena import AthenaExecuter
from mlconfig import MlConfig
from tqdm.notebook import tqdm
from collections import defaultdict
import pandas as pd


class  MatchProvider:
    def __init__(self):
        self.aurora_executor=AuroraExecutor()
        self.conf=MlConfig()
        db_name=self.conf.getValue('main','not_match_db')
        self.athena_executor=AthenaExecuter(db_name)
        

    def init_customer_matches(self):
        store=self.conf.getValue('main','store')
        customer_matches_query=self._get_customer_matches_query(store)
        df=self.aurora_executor.execute(customer_matches_query)
        df['sku_uuid_a'] = df['base_sku'].astype(str) + '<>' + \
                                df['base_source_store'].str.split('_').str[0] + '<>' + \
                                df['base_source_store'].str.split('_').str[1] 

        df['sku_uuid_b'] = df['comp_sku'].astype(str) + '<>' + \
                                    df['comp_source_store'].str.split('_').str[0] + '<>' + \
                                    df['comp_source_store'].str.split('_').str[1] 
        
        self.match_df=df

    def _get_customer_matches_query(self,store):
        table = "matches"
        
        cols = "company_code,base_upc,base_sku,base_source_store, \
              comp_sku,match_date,comp_upc,comp_source_store, \
             match_status,model_used,customer_review_state,deleted_date, \
             base_category,tpvr_worker_choice,tpvr_manager_choice,internal_notes, \
             bungee_review_state,comp_title,comp_url,comp_price,comp_img,comp_custom_sku" 

        cond = "company_code = '{}' and \
            base_source_store = '{}'".format(store,'_'.join([store,store]))
             
        where = "{} and match_status = 'product_found' and comp_sku is not null".format(cond)
        
        sql = "SELECT {} FROM {} where {} ".format(cols, table, where)
        print('sql to fetch customer match  ',sql)
        return sql
    
    def get_active_matches(self):
        match_df=self.match_df
        active_match_df = match_df[match_df.deleted_date.isna()]
        self.active_match_df=active_match_df
        return active_match_df
    
    def get_deleted_matches(self):
        match_df=self.match_df
        deleted_match_df = match_df[~match_df.deleted_date.isna()]
        self.deleted_match_df=deleted_match_df
        deleted_matches = set()
        deleted_match_df = deleted_match_df[['sku_uuid_a','sku_uuid_b']]
    
        for ind in tqdm(deleted_match_df.index):
            deleted_matches.add((match_df.at[ind,'sku_uuid_a'].lower(),
                           match_df.at[ind,'sku_uuid_b'].lower()))

        return deleted_matches

    def get_not_matches(self):
        query="""
        select concat(lower(base_sku),'<>',replace(base_source_store,'_','<>')) as sku_uuid_a,
        concat(lower(comp_sku),'<>',replace(comp_source_store,'_','<>')) as sku_uuid_b
        from fastlane_unsuccessful_demo"""
        print('sql to fetch nomtches ',query)
        not_match_df=self.athena_executor.execute(query)
        return not_match_df
    
    def get_uuid_to_store_dict(self,match_df,nomatch_df):
        matches = set()
        all_match_df = pd.concat([match_df[['sku_uuid_a','sku_uuid_b']], nomatch_df[['sku_uuid_a','sku_uuid_b']]]).reset_index(drop=True)

        for ind in tqdm(all_match_df.index):
            matches.add((all_match_df.at[ind,'sku_uuid_a'].lower(),
                            all_match_df.at[ind,'sku_uuid_b'].lower()))
            
        match_and_nomatch_store_dict = defaultdict(set)
        for uuid_a,uuid_b in tqdm(matches):
            match_and_nomatch_store_dict[uuid_a].add(uuid_b.split('<>')[-1])
            match_and_nomatch_store_dict[uuid_b].add(uuid_a.split('<>')[-1])
        return match_and_nomatch_store_dict

