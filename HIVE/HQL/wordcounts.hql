CREATE TABLE doc6 (line STRING);
LOAD DATA LOCAL INPATH '/home/alessio/Scaricati/apache-hive-3.1.2-bin/DATA/words.txt'
 OVERWRITE INTO TABLE doc6;
CREATE TABLE word_count6 AS
SELECT wa6.word, count(1) AS count FROM
 (SELECT explode(split(line, ' ')) AS word FROM doc6) wa6
GROUP BY wa6.word
ORDER BY wa6.word;
