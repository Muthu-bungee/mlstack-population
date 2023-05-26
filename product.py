import pandas as pd
from pathlib import Path
import os
from tqdm.notebook import tqdm
from mlconfig import MlConfig

class Product_Data_Provider:
    def __init__(self):
        pass
       # ! aws s3 sync s3://ml-stack.uat/type=ml_input/domain=grocery/year=2023/month=05/day=18/ latest_prod_data_path
    def download_grocery_product_data(self):
        file_type='parquet'
        conf=MlConfig()
        latest_prod_data_path=conf.getValue('main','latest_prod_data_path')
        all_files = sorted(Path(latest_prod_data_path).iterdir(), key=os.path.getmtime,reverse=True)
        li = []
        for filename in tqdm(all_files):
            if file_type == 'parquet':
                df = pd.read_parquet(str(filename),engine='pyarrow')
            else:
                df = pd.read_csv(str(filename))
            li.append(df)
        grocery_product_data_df = pd.concat(li, axis=0, ignore_index=True)
        return grocery_product_data_df
        
    
    def get_product_data_dict(self,prod_list):
        grocery_product_data_df=self.download_grocery_product_data()
        product_data_df = grocery_product_data_df[grocery_product_data_df['uuid'].isin(prod_list)]
        product_data_dict = product_data_df.set_index('uuid').T.to_dict()
        return product_data_dict
        
