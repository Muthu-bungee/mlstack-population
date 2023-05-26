import pythena

class AthenaExecuter:    
    def __init__(self,db_name):
        print('inting db',db_name)
        self.athena_client = pythena.Athena(db_name)
        print('db iint over')
    
    def execute(self,query):
        print(query)
        df,runid=self.athena_client.execute(query)
        return df