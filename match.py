from db.aurora import AuroraExecutor
class  MatchProvider:
    def __init__(self,store):
        sql=self.getQuery(store)
        ae=AuroraExecutor()
        df=ae.execute(sql)
        df['sku_uuid_a'] = df['base_sku'].astype(str) + '<>' + \
                                df['base_source_store'].str.split('_').str[0] + '<>' + \
                                df['base_source_store'].str.split('_').str[1] 

        df['sku_uuid_b'] = df['comp_sku'].astype(str) + '<>' + \
                                    df['comp_source_store'].str.split('_').str[0] + '<>' + \
                                    df['comp_source_store'].str.split('_').str[1] 
        
        self.match_df=df
        
    def getQuery(self,store):
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
