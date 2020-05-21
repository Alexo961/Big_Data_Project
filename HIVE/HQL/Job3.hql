

DROP TABLE IF EXISTS historical_stocks;
DROP TABLE IF EXISTS historical_stock_prices9;


DROP TABLE IF EXISTS JoinFinale;






CREATE TABLE  historical_stocks(ticker STRING, cambio STRING, name STRING, sector STRING, industry STRING)

ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
TBLPROPERTIES("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '/home/alessio/Scaricati/apache-hive-3.1.2-bin/DATA/historical_stocks.csv' OVERWRITE INTO TABLE historical_stocks;


CREATE TABLE  historical_stock_prices9(ticker STRING, open DOUBLE, close DOUBLE, adj_close DOUBLE, lowThe DOUBLE, highThe DOUBLE, volume INT, data DATE)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
TBLPROPERTIES("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '/home/alessio/Scaricati/apache-hive-3.1.2-bin/DATA/fileProva.csv' OVERWRITE INTO TABLE historical_stock_prices9;




CREATE TABLE IF NOT EXISTS Anno_Nomeazienda_ticker_Minanno_Maxanno AS


SELECT wa7.year AS year, wa7.ticker AS ticker,wa7.nome_azienda AS nome_azienda,MIN(wa7.data) AS data_inizio_anno,MAX(wa7.data) AS data_fine_anno FROM

(SELECT historical_stock_prices9.ticker AS ticker, YEAR(historical_stock_prices9.data) AS year,historical_stocks.name AS nome_azienda,historical_stocks.sector AS settore_azienda,historical_stock_prices9.data AS data

FROM  historical_stock_prices9 JOIN historical_stocks ON historical_stock_prices9.ticker = historical_stocks.ticker  ) wa7

WHERE wa7.year >= 2016

GROUP BY wa7.year ,wa7.ticker,wa7.nome_azienda
;



CREATE TABLE IF NOT EXISTS Anno_ticker_Minanno_chiusurainiziale_nomeazienda AS

SELECT Anno_Nomeazienda_ticker_Minanno_Maxanno.year AS year,historical_stock_prices9.ticker AS ticker,Anno_Nomeazienda_ticker_Minanno_Maxanno.data_inizio_anno AS data_inizio_anno, historical_stock_prices9.close AS chiusurainiziale , Anno_Nomeazienda_ticker_Minanno_Maxanno.nome_azienda AS nome_azienda

FROM  Anno_Nomeazienda_ticker_Minanno_Maxanno JOIN historical_stock_prices9 ON Anno_Nomeazienda_ticker_Minanno_Maxanno.data_inizio_anno = historical_stock_prices9.data AND  Anno_Nomeazienda_ticker_Minanno_Maxanno.ticker = historical_stock_prices9.ticker 


;




CREATE TABLE IF NOT EXISTS Anno_ticker_Maxanno_chiusurafinale_nomeazienda AS

SELECT Anno_Nomeazienda_ticker_Minanno_Maxanno.year AS year,historical_stock_prices9.ticker AS ticker,Anno_Nomeazienda_ticker_Minanno_Maxanno.data_fine_anno AS data_fine_anno, historical_stock_prices9.close AS chiusurafinale, Anno_Nomeazienda_ticker_Minanno_Maxanno.nome_azienda AS nome_azienda

FROM  Anno_Nomeazienda_ticker_Minanno_Maxanno JOIN historical_stock_prices9 ON Anno_Nomeazienda_ticker_Minanno_Maxanno.data_fine_anno = historical_stock_prices9.data AND  Anno_Nomeazienda_ticker_Minanno_Maxanno.ticker = historical_stock_prices9.ticker


;




CREATE TABLE IF NOT EXISTS Join_ticker_Maxanno_MinAnno AS

SELECT Anno_ticker_Minanno_chiusurainiziale_nomeazienda.year AS year,
Anno_ticker_Minanno_chiusurainiziale_nomeazienda.nome_azienda AS nome_azienda, Anno_ticker_Maxanno_chiusurafinale_nomeazienda.ticker AS ticker, Anno_ticker_Minanno_chiusurainiziale_nomeazienda.data_inizio_anno AS data_inizio_anno, Anno_ticker_Maxanno_chiusurafinale_nomeazienda.data_fine_anno AS data_fine_anno, ((Anno_ticker_Minanno_chiusurainiziale_nomeazienda.chiusurainiziale - Anno_ticker_Maxanno_chiusurafinale_nomeazienda.chiusurafinale)/Anno_ticker_Minanno_chiusurainiziale_nomeazienda.chiusurainiziale) * 100 AS diff_quotazione

FROM  Anno_ticker_Minanno_chiusurainiziale_nomeazienda JOIN Anno_ticker_Maxanno_chiusurafinale_nomeazienda ON Anno_ticker_Minanno_chiusurainiziale_nomeazienda.year = Anno_ticker_Maxanno_chiusurafinale_nomeazienda.year AND Anno_ticker_Minanno_chiusurainiziale_nomeazienda.ticker = Anno_ticker_Maxanno_chiusurafinale_nomeazienda.ticker AND
Anno_ticker_Minanno_chiusurainiziale_nomeazienda.nome_azienda = Anno_ticker_Maxanno_chiusurafinale_nomeazienda.nome_azienda


;







CREATE TABLE IF NOT EXISTS JoinFinale AS

SELECT wa9.year, CONCAT_WS(';',COLLECT_SET(wa9.nome_azienda)),wa9.variazione_annuale_media FROM

(SELECT wa8.year AS year,wa8.nome_azienda AS nome_azienda,ROUND(AVG(wa8.diff_quotazione),0) AS variazione_annuale_media FROM

(SELECT Join_ticker_Maxanno_MinAnno.year AS year, Join_ticker_Maxanno_MinAnno.diff_quotazione AS diff_quotazione, Join_ticker_Maxanno_MinAnno.nome_azienda AS nome_azienda
FROM Join_ticker_Maxanno_MinAnno
) wa8

GROUP BY wa8.year, wa8.nome_azienda)wa9

GROUP BY wa9.year, wa9.variazione_annuale_media

;








