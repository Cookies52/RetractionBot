CREATE TABLE `retractions` (
  `timestamp` TIMESTAMP NOT NULL,
  `origin` varbinary(20) NOT NULL,
  `original_doi` varbinary(200) NOT NULL,
  `retraction_doi` varbinary(200) NOT NULL,
  `original_pmed` varbinary(200) NOT NULL,
  `retraction_pmed` varbinary(200) NOT NULL,
  `retraction_nature` varbinary(200) NOT NULL,
  `url` varbinary(5000) NOT NULL
) ENGINE=Aria;

CREATE TABLE `edit_log` (
  `timestamp` TIMESTAMP NOT NULL,
  `domain` varbinary(20) NOT NULL,
  `page_title` varbinary(255) NOT NULL,
  `original_doi` varbinary(200) NOT NULL,
  `retraction_doi` varbinary(200) NOT NULL,
  `original_pmed` varbinary(200) NOT NULL,
  `retraction_pmed` varbinary(200) NOT NULL
) ENGINE=Aria;