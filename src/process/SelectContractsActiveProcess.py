
class SelectContractsActiveProcess(object):
    
    def __init__(self):
        pass

    def process(self, df_contract):
        return df_contract.filter("is_active = true")