CREATE TABLE `retractions` (
  `timestamp` TIMESTAMP NOT NULL,
  `origin` varbinary(20) NOT NULL,
  `original_doi` varbinary(200) NOT NULL,
  `retraction_doi` varbinary(200) NOT NULL,
  `original_pmid` varbinary(200) NOT NULL,
  `retraction_pmid` varbinary(200) NOT NULL,
  `retraction_nature` varbinary(200) NOT NULL,
  `url` varbinary(5000) NOT NULL,
  `notice_date` DATE NULL,
  KEY `original_doi` (`original_doi`),
  KEY `original_pmid` (`original_pmid`)
) ENGINE=Aria;

CREATE TABLE `edit_log` (
  `timestamp` TIMESTAMP NOT NULL,
  `domain` varbinary(20) NOT NULL,
  `page_title` varbinary(255) NOT NULL,
  `original_doi` varbinary(200) NOT NULL,
  `retraction_doi` varbinary(200) NOT NULL,
  `original_pmid` varbinary(200) NOT NULL,
  `retraction_pmid` varbinary(200) NOT NULL
) ENGINE=Aria;

-- Existing installations: the lookups in db.py filter on these two columns and
-- scanned the whole table for every citation they inspected. Run once:
--
--   ALTER TABLE retractions
--     ADD KEY `original_doi` (`original_doi`),
--     ADD KEY `original_pmid` (`original_pmid`);
--
-- find_retractions.py reloads the data into `retractions_new` (created with
-- CREATE TABLE ... LIKE, so it inherits these keys) and swaps it in with an
-- atomic RENAME TABLE. It also uses `retractions_old` briefly.
