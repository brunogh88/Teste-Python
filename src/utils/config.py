def config(key):
    """
    Classe responsavel por carregar configuração para o projeto

    :param key: chave da configurações
    :return: valor da configurações
    """
    config = {
                "APP_NAME" : 'Acquire-LTDA',
                "CSV_FORMAT": "csv",
                "JDBC_FORMAT": "jdbc",
                "PATH_CSV_CONTRACT": "./src/resources/contract/",
                "PATH_CSV_TRANSACTION": "./src/resources/transaction/",
                "PATH_PARQUET_RESULT": "./src/resources/result/",
                "APPEND_MODE" : "append",
                "PARQUET_FORMAT" : "parquet",
                "OVERWRITE_MODE" : "Overwrite"
    }
    return config[key]