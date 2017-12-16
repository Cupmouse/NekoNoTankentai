DELIMITER :

CREATE FUNCTION `NEKH`(
  `param` BLOB


)
  RETURNS mediumtext CHARSET utf8
LANGUAGE SQL
DETERMINISTIC
NO SQL
  SQL SECURITY INVOKER
  COMMENT 'Inputs data, returns nekonium format hex string'
  BEGIN
    RETURN CONCAT('0x', LOWER(HEX(param))):
  END;

DELIMITER ;




CREATE TABLE `address` (
  `internal_id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
  `address` BINARY(20) NOT NULL,
  `type` ENUM('NORMAL','CONTRACT') NOT NULL,
  PRIMARY KEY (`internal_id`),
  UNIQUE INDEX `address` (`address`)
)
  COLLATE='utf8_general_ci'
  ENGINE=InnoDB
;




CREATE TABLE `blocks` (
  `internal_id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'This is not block number, nessesary to giving an id for forked blocks',
  `number` INT(11) UNSIGNED NOT NULL,
  `hash` BINARY(32) NOT NULL,
  `parent` INT(10) UNSIGNED NULL DEFAULT NULL COMMENT 'Parent block\'s internal_id',
  `timestamp` DATETIME NOT NULL,
  `miner_addr_id` INT(10) UNSIGNED NOT NULL,
  `difficulty` BIGINT(20) UNSIGNED NOT NULL,
  `gas_limit` INT(11) UNSIGNED NOT NULL,
  `gas_used` INT(11) UNSIGNED NOT NULL,
  `extra_data` VARBINARY(32) NOT NULL,
  `nonce` BIGINT(20) UNSIGNED NOT NULL,
  `size` SMALLINT(5) UNSIGNED NOT NULL,
  `forked` BIT(1) NOT NULL,
  PRIMARY KEY (`internal_id`),
  INDEX `miner` (`miner_addr_id`),
  INDEX `number` (`number`),
  INDEX `hash` (`hash`),
  INDEX `timestamp` (`timestamp`),
  INDEX `FK_blocks_blocks1` (`parent`),
  INDEX `forked` (`forked`),
  CONSTRAINT `FK_blocks_address` FOREIGN KEY (`miner_addr_id`) REFERENCES `address` (`internal_id`) ON UPDATE CASCADE ON DELETE CASCADE,
  CONSTRAINT `FK_blocks_blocks_` FOREIGN KEY (`parent`) REFERENCES `blocks` (`internal_id`) ON UPDATE CASCADE ON DELETE CASCADE
)
  COLLATE='utf8_general_ci'
  ENGINE=InnoDB
;




CREATE TABLE `uncle_blocks` (
  `internal_id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'This uncle\'s internal id, for giving an individual id for forked blocks',
  `number` INT(11) UNSIGNED NOT NULL COMMENT 'This uncle\'s block number',
  `block_id` INT(10) UNSIGNED NOT NULL COMMENT 'The block internal id in which this uncle block is included. Referencing number is NOT blockNumber but blocks.internal_id',
  `index` TINYINT(4) UNSIGNED NOT NULL COMMENT 'This uncle\'s uncle index in the included block',
  `hash` BINARY(32) NOT NULL,
  `parent` INT(10) UNSIGNED NOT NULL COMMENT 'Parent block\'s internal_id',
  `timestamp` DATETIME NOT NULL,
  `miner_id` INT(10) UNSIGNED NOT NULL,
  `difficulty` BIGINT(20) UNSIGNED NOT NULL,
  `gas_limit` INT(11) UNSIGNED NOT NULL,
  `gas_used` INT(11) UNSIGNED NOT NULL,
  `extra_data` VARBINARY(32) NOT NULL,
  `nonce` BIGINT(20) UNSIGNED NOT NULL,
  `size` SMALLINT(5) UNSIGNED NOT NULL,
  PRIMARY KEY (`internal_id`),
  INDEX `hash` (`hash`),
  INDEX `number` (`number`),
  INDEX `index` (`index`),
  INDEX `FK_uncle_blocks_blocks` (`block_id`),
  INDEX `FK_uncle_blocks_blocks_2` (`parent`),
  INDEX `FK_uncle_blocks_address` (`miner_id`),
  CONSTRAINT `FK_uncle_blocks_address` FOREIGN KEY (`miner_id`) REFERENCES `address` (`internal_id`),
  CONSTRAINT `FK_uncle_blocks_blocks` FOREIGN KEY (`block_id`) REFERENCES `blocks` (`internal_id`) ON UPDATE CASCADE ON DELETE CASCADE,
  CONSTRAINT `FK_uncle_blocks_blocks_2` FOREIGN KEY (`parent`) REFERENCES `blocks` (`internal_id`) ON UPDATE CASCADE ON DELETE CASCADE
)
  COLLATE='utf8_general_ci'
  ENGINE=InnoDB
;




CREATE TABLE `transactions` (
  `internal_id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'Internal id exists for giving every blocks an id, not only main chain one, but also forked blocks',
  `block_id` INT(10) UNSIGNED NOT NULL COMMENT 'Block\'s internal id that this transaction is included in',
  `index` SMALLINT(5) UNSIGNED NOT NULL COMMENT 'Transaction index number on the block that this transaction is included in. Smaller the number is, it has to be processed earlier from the same blocks ones.',
  `hash` BINARY(32) NOT NULL,
  `from_id` INT(10) UNSIGNED NOT NULL,
  `to_id` INT(10) UNSIGNED NULL DEFAULT NULL,
  `contract_id` INT(10) UNSIGNED NULL DEFAULT NULL,
  `value` VARBINARY(16) NOT NULL,
  `gas_provided` MEDIUMINT(8) UNSIGNED NOT NULL,
  `gas_used` MEDIUMINT(8) UNSIGNED NOT NULL,
  `gas_price` VARBINARY(16) NOT NULL,
  `nonce` BIGINT(20) UNSIGNED NOT NULL,
  `input` BLOB NOT NULL,
  PRIMARY KEY (`internal_id`),
  INDEX `to` (`to_id`),
  INDEX `from` (`from_id`),
  INDEX `hash` (`hash`),
  INDEX `FK_transactions_blocks` (`block_id`),
  INDEX `block_id_index` (`block_id`, `index`),
  INDEX `FK_transactions_address_3` (`contract_id`),
  CONSTRAINT `FK_transactions_address` FOREIGN KEY (`from_id`) REFERENCES `address` (`internal_id`) ON UPDATE CASCADE ON DELETE CASCADE,
  CONSTRAINT `FK_transactions_address_2` FOREIGN KEY (`to_id`) REFERENCES `address` (`internal_id`) ON UPDATE CASCADE ON DELETE CASCADE,
  CONSTRAINT `FK_transactions_address_3` FOREIGN KEY (`contract_id`) REFERENCES `address` (`internal_id`) ON UPDATE CASCADE ON DELETE CASCADE,
  CONSTRAINT `FK_transactions_blocks` FOREIGN KEY (`block_id`) REFERENCES `blocks` (`internal_id`) ON UPDATE CASCADE ON DELETE CASCADE
)
  COLLATE='utf8_general_ci'
  ENGINE=InnoDB
;




CREATE TABLE `balance` (
  `block_id` INT(11) UNSIGNED NOT NULL,
  `address_id` INT(10) UNSIGNED NOT NULL,
  `balance` VARBINARY(16) NOT NULL,
  INDEX `FK_balance_blocks` (`block_id`),
  INDEX `address_id` (`address_id`),
  CONSTRAINT `FK_balance_address` FOREIGN KEY (`address_id`) REFERENCES `address` (`internal_id`),
  CONSTRAINT `FK_balance_blocks` FOREIGN KEY (`block_id`) REFERENCES `blocks` (`internal_id`) ON UPDATE CASCADE ON DELETE CASCADE
)
  COLLATE='utf8_general_ci'
  ENGINE=InnoDB
;




CREATE TABLE `balance_changes` (
  `block_id` INT(10) UNSIGNED NOT NULL,
  `address_id` INT(10) UNSIGNED NOT NULL,
  `negative` BIT(1) NOT NULL,
  `delta` VARBINARY(16) NOT NULL,
  INDEX `FK_balance_changes_address` (`address_id`),
  INDEX `FK_addresses_transactions` (`block_id`),
  CONSTRAINT `FK_addresses_transactions` FOREIGN KEY (`block_id`) REFERENCES `blocks` (`internal_id`),
  CONSTRAINT `FK_balance_changes_address` FOREIGN KEY (`address_id`) REFERENCES `address` (`internal_id`)
)
  COLLATE='utf8_general_ci'
  ENGINE=InnoDB
;
