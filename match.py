from db.aurora import AuroraExecutor
from db.athena import AthenaExecuter
from mlconfig import MlConfig


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
        
        sql = "SELECT {} FROM {} where {} limit 10".format(cols, table, where)
        
        print('sql is ',sql)
        return sql
    
    def get_active_matches(self):
        match_df=self.match_df
        active_match_df = match_df[match_df.deleted_date.isna()]
        print(f'Final active matches from match library {active_match_df.shape[0]}')
        self.active_match_df=active_match_df
        return active_match_df
    
    def get_deleted_matches(self):
        match_df=self.match_df
        deleted_match_df = match_df[~match_df.deleted_date.isna()]
        print(f'Final deleted_match from match library {deleted_match_df.shape[0]}')
        self.deleted_match_df=deleted_match_df
        return deleted_match_df

    def get_not_matches(self):
        query="""
        select concat(lower(base_sku),'<>',replace(base_source_store,'_','<>')) as sku_uuid_a,
        concat(lower(comp_sku),'<>',replace(comp_source_store,'_','<>')) as sku_uuid_b
        from fastlane_unsuccessful_demo"""
        df=self.athena_executor.execute(query)
        print('unsuccess matches',df.shape)
        return df

