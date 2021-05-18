# Inicial
Na pasta raiz rode o comando abaixo para preparar o ambiente Oracle:
    docker-compose up

Para Executar o programa na pasta raiz rode o seguinte comando:
    
    Windows:
        spark-submit.cmd --master local[2] --packages "com.oracle.database.jdbc:ojdbc6:11.2.0.4"  main.py --date 20210517 --pipe ORACLE

        ou

        spark-submit.cmd --master local[2] --packages "com.oracle.database.jdbc:ojdbc6:11.2.0.4"  main.py --date 20210517 --pipe CSV

    Linux
        spark-submit --master local[2] --packages "com.oracle.database.jdbc:ojdbc6:11.2.0.4"  main.py --date 20210517 --pipe ORACLE

        ou

        spark-submit --master local[2] --packages "com.oracle.database.jdbc:ojdbc6:11.2.0.4"  main.py --date 20210517 --pipe CSV