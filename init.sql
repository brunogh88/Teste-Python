CREATE USER DW IDENTIFIED BY test;

GRANT ALL PRIVILEGES TO DW;

CREATE TABLE DW.TB_TRANSACTION (
    TRANSACTION_ID NUMBER,
    CLIENT_ID NUMBER,
    TOTAL_AMOUNT NUMBER,
    DISCOUNT_PERCENTAGE NUMBER(10,2)
);

INSERT INTO DW.TB_TRANSACTION VALUES(1, 3545, 3000, 6.99);
INSERT INTO DW.TB_TRANSACTION VALUES(2, 3545, 4500, 0.45);
INSERT INTO DW.TB_TRANSACTION VALUES(3, 3509, 69998, 0);
INSERT INTO DW.TB_TRANSACTION VALUES(4, 3510, 1, NULL);
INSERT INTO DW.TB_TRANSACTION VALUES(5, 4510, 34, 40);
COMMIT;

CREATE TABLE DW.TB_CONTRACT (
    CONTRACT_ID NUMBER,
    CLIENT_ID NUMBER,
    CLIENT_NAME VARCHAR2(100),
    PERCENTAGE NUMBER(5,2),
    IS_ACTIVE VARCHAR2(5)
);

INSERT INTO DW.TB_CONTRACT VALUES(3, 3545, 'Magazine Luana', 2.00, 'true');
INSERT INTO DW.TB_CONTRACT VALUES(4, 3545, 'Magazine Luana', 1.95, 'false');
INSERT INTO DW.TB_CONTRACT VALUES(5, 3509, 'Lojas Italianas', 1, 'true');
INSERT INTO DW.TB_CONTRACT VALUES(6, 3510, 'Carrerfive', 3.00, 'true');
COMMIT;

CREATE TABLE DW.TB_FINAL(
    VL_PROFIT NUMBER(10,3),
    DT_PROCESS DATE
);