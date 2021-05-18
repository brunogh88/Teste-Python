class UnionContractTransactionProcess(object):
    
    def __init__(self):
        pass

    def process(self, df_contract, df_transaction):
        return (
            df_contract
            .join(df_transaction, df_contract.client_id == df_transaction.client_id, "INNER")
            .drop(df_contract.client_id)
            .drop(df_contract.dt_partition)
        )