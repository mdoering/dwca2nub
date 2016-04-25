# dwca2nub
Tool to update a dwca with the GBIF backbone taxonIDs


## GBIF Occurrence Query

 - create table ```mdoering.giasip_taxon_keys``` with all taxon keys from the archive
 - execute this hive query:
```
CREATE TABLE mdoering.gbif_country
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
AS

SELECT occ.taxonKey, occ.countrycode, count(*) as cnt FROM (

SELECT o.taxonKey, o.countrycode, count(*) as cnt
FROM uat.occurrence_hdfs o
  JOIN mdoering.giasip_taxon_keys k ON k.key=o.taxonKey
WHERE
  o.eventDate IS NOT NULL AND year > 1900 AND
  o.decimalLatitude IS NOT NULL AND o.decimalLongitude IS NOT NULL AND
  o.hasGeospatialIssues = false AND
  o.countrycode IS NOT NULL AND
  o.basisOfRecord IN('PRESERVED_SPECIMEN', 'OBSERVATION', 'HUMAN_OBSERVATION', 'MACHINE_OBSERVATION')

UNION ALL

SELECT o.taxonKey, o.countrycode, count(*) as cnt
FROM uat.occurrence_hdfs o
  JOIN mdoering.giasip_taxon_keys k ON k.key=o.speciesKey
WHERE
  o.eventDate IS NOT NULL AND year > 1900 AND
  o.decimalLatitude IS NOT NULL AND o.decimalLongitude IS NOT NULL AND
  o.hasGeospatialIssues = false AND
  o.countrycode IS NOT NULL AND
  o.basisOfRecord IN('PRESERVED_SPECIMEN', 'OBSERVATION', 'HUMAN_OBSERVATION', 'MACHINE_OBSERVATION')

UNION ALL

SELECT o.taxonKey, o.countrycode, count(*) as cnt
FROM uat.occurrence_hdfs o
  JOIN mdoering.giasip_taxon_keys k ON k.key=o.genusKey
WHERE
  o.eventDate IS NOT NULL AND year > 1900 AND
  o.decimalLatitude IS NOT NULL AND o.decimalLongitude IS NOT NULL AND
  o.hasGeospatialIssues = false AND
  o.countrycode IS NOT NULL AND
  o.basisOfRecord IN('PRESERVED_SPECIMEN', 'OBSERVATION', 'HUMAN_OBSERVATION', 'MACHINE_OBSERVATION')
) AS occ

GROUP BY countrycode, taxonKey
ORDER BY taxonKey, countrycode
```
 - ```ssh root@prodgateway-vh.gbif.orrg```
 - ```hdfs dfs -getmerge /user/hive/warehouse/mdoering.db/gbif_country /tmp/gbif_country.csv```
