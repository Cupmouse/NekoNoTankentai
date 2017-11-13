CREATE TABLE `blocks` (
	`internal_id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'This is not block number, nessesary to giving an id for forked blocks',
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
	`sha3_uncles` BINARY(32) NOT NULL COMMENT 'I don\'t know what this value mean',
	`size` SMALLINT(5) UNSIGNED NOT NULL,
	PRIMARY KEY (`internal_id`),
	INDEX `miner` (`miner`),
	INDEX `number` (`number`),
	INDEX `hash` (`hash`),
	INDEX `timestamp` (`timestamp`)
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
	`parent_hash` BINARY(32) NOT NULL COMMENT 'This uncle\'s parent block\'s hash, since it is an uncle block, the block on the main chain with same the block number has to have the same parent block',
	`timestamp` DATETIME NOT NULL,
	`miner` BINARY(20) NOT NULL,
	`difficulty` BIGINT(20) UNSIGNED NOT NULL,
	`gas_limit` INT(11) UNSIGNED NOT NULL,
	`gas_used` INT(11) UNSIGNED NOT NULL,
	`extra_data` VARBINARY(32) NOT NULL,
	`nonce` BIGINT(20) UNSIGNED NOT NULL,
	`sha3_uncles` BINARY(32) NOT NULL COMMENT 'Why? could there be uncle\'s uncle???',
	`size` SMALLINT(5) UNSIGNED NOT NULL,
	PRIMARY KEY (`internal_id`),
	INDEX `hash` (`hash`),
	INDEX `number` (`number`),
	INDEX `index` (`index`),
	INDEX `FK_uncle_blocks_blocks` (`block_id`),
	CONSTRAINT `FK_uncle_blocks_blocks` FOREIGN KEY (`block_id`) REFERENCES `blocks` (`internal_id`) ON UPDATE CASCADE ON DELETE CASCADE
)
	COLLATE='utf8_general_ci'
	ENGINE=InnoDB
;




CREATE TABLE `transactions` (
	`internal_id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'Internal id exists for giving every blocks an id, not only main chain one, but also forked blocks',
	`block_id` INT(10) UNSIGNED NOT NULL COMMENT 'Block\'s internal id that this transaction is included in',
	`block_number` INT(10) UNSIGNED NOT NULL COMMENT 'Block number that this transaction is included in NOT NEEDED',
	`index` SMALLINT(5) UNSIGNED NOT NULL COMMENT 'Transaction index number on the block that this transaction is included in. Smaller the number is, it has to be processed earlier from the same blocks ones.',
	`hash` BINARY(32) NOT NULL,
	`from` BINARY(20) NOT NULL,
	`to` BINARY(20) NULL DEFAULT NULL,
	`value` VARBINARY(16) NOT NULL,
	`gas` MEDIUMINT(8) UNSIGNED NOT NULL,
	`gas_price` VARBINARY(16) NOT NULL,
	`nonce` BIGINT(20) UNSIGNED NOT NULL,
	`input` BLOB NOT NULL,
	PRIMARY KEY (`internal_id`),
	INDEX `to` (`to`),
	INDEX `from` (`from`),
	INDEX `block_number` (`block_number`),
	INDEX `hash` (`hash`),
	INDEX `FK_transactions_blocks` (`block_id`),
	CONSTRAINT `FK_transactions_blocks` FOREIGN KEY (`block_id`) REFERENCES `blocks` (`internal_id`) ON UPDATE CASCADE ON DELETE CASCADE
)
	COLLATE='utf8_general_ci'
	ENGINE=InnoDB
;
