CREATE TABLE `blocks` (
  `internal_id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
  `number` INT(11) UNSIGNED NOT NULL,
  `hash` BINARY(32) NOT NULL,
  `parent_hash` BINARY(32) NOT NULL,
  `timestamp` DATETIME NOT NULL,
  `miner` BINARY(20) NOT NULL,
  `difficulty` BIGINT(20) UNSIGNED NOT NULL,
  `gas_limit` INT(11) UNSIGNED NOT NULL,
  `gas_used` INT(11) UNSIGNED NOT NULL,
  `extra_data` VARBINARY(32) NOT NULL,
  `nonce` BIGINT(20) UNSIGNED NOT NULL,
  `sha3_uncles` BINARY(32) NOT NULL,
  `size` SMALLINT(5) UNSIGNED NOT NULL,
  `logs_bloom` VARBINARY(256) NOT NULL COMMENT 'NOT NEEDED',
  `mix_hash` BINARY(32) NOT NULL COMMENT 'NOT NEEDED',
  `receipts_root` BINARY(32) NOT NULL COMMENT 'NOT NEEDED',
  `state_root` BINARY(32) NOT NULL COMMENT 'NOT NEEDED',
  `transactions_root` BINARY(32) NOT NULL COMMENT 'NOT NEEDED',
  `total_difficulty` VARBINARY(16) NOT NULL COMMENT 'NOT NEEDED',
  `transactions` LONGTEXT NOT NULL COMMENT 'NOT NEEDED',
  `uncles` LONGTEXT NOT NULL COMMENT 'NOT NEEDED',
  PRIMARY KEY (`internal_id`),
  INDEX `miner` (`miner`),
  INDEX `number` (`number`),
  INDEX `hash` (`hash`),
  INDEX `timestamp` (`timestamp`)
)
  COLLATE='utf8_general_ci'
  ENGINE=InnoDB
  AUTO_INCREMENT=36005
;


CREATE TABLE `uncle_blocks` (
  `internal_id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
  `number` INT(11) UNSIGNED NOT NULL,
  `index` TINYINT(4) UNSIGNED NOT NULL,
  `hash` BINARY(32) NOT NULL,
  `parent_hash` BINARY(32) NOT NULL,
  `timestamp` DATETIME NOT NULL,
  `miner` BINARY(20) NOT NULL,
  `difficulty` BIGINT(20) UNSIGNED NOT NULL,
  `gas_limit` INT(11) UNSIGNED NOT NULL,
  `gas_used` INT(11) UNSIGNED NOT NULL,
  `extra_data` VARBINARY(32) NOT NULL,
  `nonce` BIGINT(20) UNSIGNED NOT NULL,
  `sha3_uncles` BINARY(32) NOT NULL,
  `size` SMALLINT(5) UNSIGNED NOT NULL,
  PRIMARY KEY (`internal_id`),
  INDEX `hash` (`hash`)
)
  COLLATE='utf8_general_ci'
  ENGINE=InnoDB
  ROW_FORMAT=DYNAMIC
;


CREATE TABLE `transactions` (
  `internal_id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
  `block_number` INT(10) UNSIGNED NOT NULL,
  `index` SMALLINT(5) UNSIGNED NOT NULL,
  `hash` BINARY(32) NOT NULL,
  `from` BINARY(20) NOT NULL,
  `to` BINARY(20) NULL DEFAULT NULL,
  `value` VARBINARY(16) NOT NULL,
  `gas` MEDIUMINT(8) UNSIGNED NOT NULL,
  `gas_price` VARBINARY(16) NOT NULL,
  `nonce` BIGINT(20) UNSIGNED NOT NULL,
  `input` BLOB NOT NULL,
  `r` BINARY(32) NOT NULL COMMENT 'NOT NEEDED',
  `s` BINARY(32) NOT NULL COMMENT 'NOT NEEDED',
  `v` SMALLINT(5) UNSIGNED NOT NULL COMMENT 'NOT NEEDED',
  PRIMARY KEY (`internal_id`),
  INDEX `to` (`to`),
  INDEX `from` (`from`),
  INDEX `block_number` (`block_number`),
  INDEX `hash` (`hash`),
  CONSTRAINT `FK_transactions_blocks` FOREIGN KEY (`block_number`) REFERENCES `blocks` (`number`) ON UPDATE CASCADE ON DELETE CASCADE
)
  COLLATE='utf8_general_ci'
  ENGINE=InnoDB
;
