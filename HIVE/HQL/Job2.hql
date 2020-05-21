

DROP TABLE IF EXISTS historical_stocks;
DROP TABLE IF EXISTS historical_stock_prices9;


DROP TABLE IF EXISTS JoinFinale;
DROP TABLE IF EXISTS Anno_ticker_Minanno_Maxanno;
DROP TABLE IF EXISTS Join_ticker_Maxanno_MinAnno;
DROP TABLE IF EXISTS Variazione_annuale_media_anno;
DROP TABLE IF EXISTS Anno_ticker_Minanno_chiusurainiziale ;
DROP TABLE IF EXISTS  Anno_ticker_Maxanno_chiusurafinale;
 
 DROP TABLE IF EXISTS SupportC;



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




CREATE TABLE IF NOT EXISTS Anno_ticker_Minanno_Maxanno AS


SELECT wa7.year AS year, wa7.ticker AS ticker,MIN(wa7.data) AS data_inizio_anno,MAX(wa7.data) AS data_fine_anno FROM

(SELECT historical_stock_prices9.ticker AS ticker, YEAR(historical_stock_prices9.data) AS year,(historical_stock_prices9.close - historical_stock_prices9.open) AS quotazione_giornaliera,historical_stocks.sector AS settore_azienda,historical_stock_prices9.data AS data

FROM  historical_stock_prices9 JOIN historical_stocks ON historical_stock_prices9.ticker = historical_stocks.ticker  ) wa7

WHERE wa7.year >= 2008

GROUP BY wa7.year ,wa7.ticker
;



CREATE TABLE IF NOT EXISTS Anno_ticker_Minanno_chiusurainiziale AS

SELECT Anno_ticker_Minanno_Maxanno.year AS year,historical_stock_prices9.ticker AS ticker,Anno_ticker_Minanno_Maxanno.data_inizio_anno AS data_inizio_anno, historical_stock_prices9.close AS chiusurainiziale

FROM  Anno_ticker_Minanno_Maxanno JOIN historical_stock_prices9 ON Anno_ticker_Minanno_Maxanno.data_inizio_anno = historical_stock_prices9.data AND  Anno_ticker_Minanno_Maxanno.ticker = historical_stock_prices9.ticker


;




CREATE TABLE IF NOT EXISTS Anno_ticker_Maxanno_chiusurafinale AS

SELECT Anno_ticker_Minanno_Maxanno.year AS year,historical_stock_prices9.ticker AS ticker,Anno_ticker_Minanno_Maxanno.data_fine_anno AS data_fine_anno, historical_stock_prices9.close AS chiusurafinale

FROM  Anno_ticker_Minanno_Maxanno JOIN historical_stock_prices9 ON Anno_ticker_Minanno_Maxanno.data_fine_anno = historical_stock_prices9.data AND  Anno_ticker_Minanno_Maxanno.ticker = historical_stock_prices9.ticker


;




CREATE TABLE IF NOT EXISTS Join_ticker_Maxanno_MinAnno AS

SELECT Anno_ticker_Minanno_chiusurainiziale.year AS year, Anno_ticker_Maxanno_chiusurafinale.ticker AS ticker, Anno_ticker_Minanno_chiusurainiziale.data_inizio_anno AS data_inizio_anno, Anno_ticker_Maxanno_chiusurafinale.data_fine_anno AS data_fine_anno, ((Anno_ticker_Minanno_chiusurainiziale.chiusurainiziale - Anno_ticker_Maxanno_chiusurafinale.chiusurafinale)/Anno_ticker_Minanno_chiusurainiziale.chiusurainiziale) * 100 AS diff_quotazione

FROM  Anno_ticker_Minanno_chiusurainiziale JOIN Anno_ticker_Maxanno_chiusurafinale ON Anno_ticker_Minanno_chiusurainiziale.year = Anno_ticker_Maxanno_chiusurafinale.year AND Anno_ticker_Minanno_chiusurainiziale.ticker = Anno_ticker_Maxanno_chiusurafinale.ticker


;


CREATE TABLE IF NOT EXISTS SupportC AS


SELECT wa6.year AS year, wa6.ticker AS ticker,wa6.settore_azienda AS settore_azienda,AVG(wa6.volume) AS volume_annuale_medio, AVG(wa6.quotazione_giornaliera) AS quotazione_giornaliera_media FROM

(SELECT historical_stock_prices9.ticker AS ticker, YEAR(historical_stock_prices9.data) AS year,historical_stock_prices9.volume AS volume ,(historical_stock_prices9.close - historical_stock_prices9.open) AS quotazione_giornaliera,historical_stocks.sector AS settore_azienda

FROM  historical_stock_prices9 JOIN historical_stocks ON historical_stock_prices9.ticker = historical_stocks.ticker  ) wa6

WHERE wa6.year >= 2008

GROUP BY wa6.year ,wa6.ticker, wa6.settore_azienda
;




CREATE TABLE IF NOT EXISTS JoinFinale AS

SELECT wa8.year,wa8.settore_azienda,ROUND (AVG(wa8.volume_annuale_medio),0) AS volume_medio,ROUND(AVG(wa8.diff_quotazione),0) AS variazione_annuale_media,ROUND(AVG(wa8.quotazione_giornaliera_media),0) AS quotazione_giornaliera_media FROM

(SELECT SupportC.year AS year,SupportC.ticker AS ticker, SupportC.settore_azienda AS settore_azienda, SupportC.volume_annuale_medio AS volume_annuale_medio , Join_ticker_Maxanno_MinAnno.diff_quotazione AS diff_quotazione, SupportC.quotazione_giornaliera_media AS quotazione_giornaliera_media 

FROM Join_ticker_Maxanno_MinAnno JOIN SupportC ON  Join_ticker_Maxanno_MinAnno.year = SupportC.year AND Join_ticker_Maxanno_MinAnno.ticker = SupportC.ticker) wa8

GROUP BY wa8.year, wa8.settore_azienda

;









