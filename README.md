# dwca2nub
Tool to update a dwca with the GBIF backbone taxonIDs


## GBIF Occurrence Query

 - create table ```mdoering.giasip_taxon_keys``` with all taxon keys from the archive
 - execute this hive query:
```
CREATE TABLE mdoering.gbif_country
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
AS

SELECT occ.taxonKey, occ.countrycode, count(*) as cnt
FROM (

SELECT k.key AS taxonKey, o.countrycode, o.eventDate, o.year, o.hasGeospatialIssues, o.decimalLatitude, o.decimalLongitude, o.basisOfRecord
FROM uat.occurrence_hdfs o JOIN mdoering.giasip_taxon_keys k ON k.key=o.taxonKey

UNION ALL
SELECT k.key AS taxonKey, o.countrycode, o.eventDate, o.year, o.hasGeospatialIssues, o.decimalLatitude, o.decimalLongitude, o.basisOfRecord
FROM uat.occurrence_hdfs o JOIN mdoering.giasip_taxon_keys k ON k.key=o.speciesKey

UNION ALL
SELECT k.key AS taxonKey, o.countrycode, o.eventDate, o.year, o.hasGeospatialIssues, o.decimalLatitude, o.decimalLongitude, o.basisOfRecord
FROM uat.occurrence_hdfs o JOIN mdoering.giasip_taxon_keys k ON k.key=o.genusKey
) AS occ

WHERE
  occ.eventDate IS NOT NULL AND year > 1900
  AND occ.decimalLatitude IS NOT NULL AND occ.decimalLongitude IS NOT NULL
  AND occ.hasGeospatialIssues = false
  AND occ.countrycode IS NOT NULL AND length(occ.countrycode) > 0
  AND occ.basisOfRecord IN('PRESERVED_SPECIMEN', 'OBSERVATION', 'HUMAN_OBSERVATION', 'MACHINE_OBSERVATION')
GROUP BY countrycode, taxonKey
ORDER BY taxonKey, countrycode
```
 - ```ssh root@prodgateway-vh.gbif.orrg```
 - ```hdfs dfs -getmerge /user/hive/warehouse/mdoering.db/gbif_country /tmp/gbif-country.csv```
