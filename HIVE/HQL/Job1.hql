


DROP TABLE IF EXISTS historical_stock_prices9;


DROP TABLE IF EXISTS Date_supportA;
DROP TABLE IF EXISTS Date_supportB;
DROP TABLE IF EXISTS SupportA;
DROP TABLE IF EXISTS SupportB;
DROP TABLE IF EXISTS Tabbellona ;
DROP TABLE IF EXISTS  Tabbellona1;
DROP TABLE IF EXISTS  Job1_1;





CREATE TABLE  historical_stock_prices9(ticker STRING, open DOUBLE, close DOUBLE, adj_close DOUBLE, lowThe DOUBLE, highThe DOUBLE, volume INT, data DATE)

ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
TBLPROPERTIES("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '/home/alessio/Scaricati/apache-hive-3.1.2-bin/DATA/csv_min.csv' OVERWRITE INTO TABLE historical_stock_prices9;




CREATE TABLE IF NOT EXISTS Date_supportA AS

(SELECT ticker,MIN(data) AS data_chiusura_iniziale

FROM historical_stock_prices9

GROUP BY ticker);


CREATE TABLE IF NOT EXISTS Date_supportB AS

(SELECT ticker,MAX(data) AS data_chiusura_finale

FROM historical_stock_prices9

GROUP BY ticker);





CREATE TABLE IF NOT EXISTS SupportA AS

(SELECT historical_stock_prices9.ticker AS ticker, historical_stock_prices9.close AS prezzo_chiusura_iniziale

FROM  Date_supportA JOIN historical_stock_prices9 ON Date_supportA.data_chiusura_iniziale = historical_stock_prices9.data AND Date_supportA.ticker= historical_stock_prices9.ticker);


CREATE TABLE IF NOT EXISTS SupportB AS

(SELECT historical_stock_prices9.ticker AS ticker, historical_stock_prices9.close AS prezzo_chiusura_finale

FROM  Date_supportB JOIN historical_stock_prices9 ON Date_supportB.data_chiusura_finale = historical_stock_prices9.data AND Date_supportB.ticker= historical_stock_prices9.ticker);



CREATE TABLE IF NOT EXISTS Tabbellona AS

(SELECT SupportA.ticker AS ticker , SupportA.prezzo_chiusura_iniziale AS prezzo_chiusura_iniziale , SupportB.prezzo_chiusura_finale AS prezzo_chiusura_finale

FROM  SupportA JOIN SupportB ON SupportB.ticker = SupportA.ticker);



CREATE TABLE IF NOT EXISTS Tabbellona1 AS

(SELECT ticker, MIN(lowThe) AS prezzo_minimo ,MAX(highThe) AS prezzo_massimo,AVG(volume) AS volume_medio


FROM   historical_stock_prices9 


GROUP BY  historical_stock_prices9.ticker

);

CREATE TABLE Job1_1 AS

(SELECT Tabbellona1.ticker AS ticker , ROUND(Tabbellona1.prezzo_minimo ,0), ROUND(Tabbellona1.prezzo_massimo ,0), ROUND(Tabbellona1.volume_medio ,0),  ROUND((((Tabbellona.prezzo_chiusura_iniziale - Tabbellona.prezzo_chiusura_finale)/Tabbellona.prezzo_chiusura_iniziale) *100) , 0) AS variazione_quotazione

FROM  Tabbellona JOIN Tabbellona1 ON Tabbellona.ticker = Tabbellona1.ticker);






