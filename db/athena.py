import pythena

class AthenaExecuter:    
    def __init__(self,db_name):
        self.athena_client = pythena.Athena(db_name)
    
    def execute(self,query):
        df,runid=self.athena_client.execute(query)
        return df