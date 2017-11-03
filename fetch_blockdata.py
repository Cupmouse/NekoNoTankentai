# coding: UTF-8

from web3 import Web3, KeepAliveRPCProvider
import pymysql.cursors
import json
from time import sleep
import sys

def init_web3():
	global web3
	web3 = Web3(KeepAliveRPCProvider(host='localhost', port='8293'))

def init_db():
	global conn
	
	# mysql接続する
	conn = pymysql.connect(
		host='localhost',
		user="root",
		password="p37suvmq",
		db='nekonium'
	)

	# オートコミットはオフ
	conn.autocommit(False)
	
	# テーブルを作成する
	c = conn.cursor()
	
	c.execute("""
		CREATE TABLE IF NOT EXISTS `blocks` (
			`difficulty` BIGINT(20) UNSIGNED NOT NULL,
			`extra_data` VARBINARY(32) NOT NULL,
			`gas_limit` INT(11) UNSIGNED NOT NULL,
			`gas_used` INT(11) UNSIGNED NOT NULL,
			`hash` BINARY(32) NOT NULL,
			`logs_bloom` VARBINARY(256) NOT NULL,
			`miner` BINARY(20) NOT NULL,
			`mix_hash` BINARY(32) NOT NULL,
			`nonce` BINARY(8) NOT NULL,
			`number` INT(11) UNSIGNED NOT NULL,
			`parent_hash` BINARY(32) NOT NULL,
			`receipts_root` BINARY(32) NOT NULL,
			`sha3_uncles` BINARY(32) NOT NULL,
			`size` SMALLINT(5) UNSIGNED NOT NULL,
			`state_root` BINARY(32) NOT NULL,
			`timestamp` DATETIME NOT NULL,
			`total_difficulty` BIGINT(20) UNSIGNED NOT NULL,
			`transactions` MEDIUMTEXT NULL DEFAULT NULL,
			`transactions_root` BINARY(32) NOT NULL,
			`uncles` TEXT NULL DEFAULT NULL,
			PRIMARY KEY (`number`)
		)
		COLLATE='utf8_general_ci'
		ENGINE=InnoDB
		;
		""")
	c.execute("""
		CREATE TABLE IF NOT EXISTS `transactions` (
			`block_number` INT(10) UNSIGNED NOT NULL,
			`from` BINARY(20) NOT NULL,
			`gas` MEDIUMINT(8) UNSIGNED NOT NULL,
			`gas_price` VARBINARY(16) NOT NULL,
			`hash` BINARY(32) NOT NULL,
			`input` BLOB NOT NULL,
			`nonce` INT(10) UNSIGNED NOT NULL,
			`r` BINARY(32) NOT NULL,
			`s` BINARY(32) NOT NULL,
			`to` BINARY(20) NULL DEFAULT NULL,
			`transaction_index` SMALLINT(5) UNSIGNED NOT NULL,
			`v` BINARY(2) NOT NULL,
			`value` VARBINARY(16) NOT NULL
		)
		COLLATE='utf8_general_ci'
		ENGINE=InnoDB
		;
	""")
	conn.commit()
	c.close()

def insert_tx(t):
	print('new tx: %s' % t['hash'])

	c = conn.cursor()

	# コントラクトのときはtoがない
	to = t['to']
	
	if to is not None:
		to = to[2:]

	c.execute("INSERT INTO transactions VALUES (%s, UNHEX(%s), %s, UNHEX(%s), UNHEX(%s), UNHEX(%s), %s, UNHEX(%s), UNHEX(%s), UNHEX(%s), %s, UNHEX(%s), UNHEX(%s))",
		(t['blockNumber'],
		 t['from'][2:],
		 t['gas'],
		 t['gasPrice'],
		 t['hash'][2:],
		 t['input'][2:],
		 t['nonce'],
		 t['r'][2:],
		 t['s'][2:],
		 to,
		 t['transactionIndex'],
		 t['v'][2:],
		 t['value'])
	)

	c.close()

def fetch_block(blockIdentifier, expected_num):
	if isinstance(blockIdentifier, str):
		print('fetching block with hash: %s' % blockIdentifier)
	else:
		print('fetching block with number: %d' % blockIdentifier)

	try:
		c = conn.cursor()

		r = web3.eth.getBlock(blockIdentifier, True)
		
		tsp = r['timestamp']
		if tsp == 0:
			tsp = 1
		 
		if len(r['transactions']) == 0:
			txhashes = None
		else:
			txhashes = []
			for i in range(len(r['transactions'])):
				txhashes.append(r['transactions'][i]['hash'])
				insert_tx(r['transactions'][i])

			txhashes = json.dumps(txhashes)
		
		unc = r['uncles']
		if len(unc) == 0:
			unc = None
		else:
			unc = json.dumps(unc)
		
		c.execute("INSERT INTO blocks VALUES (%s, UNHEX(%s), %s, %s, UNHEX(%s), UNHEX(%s), UNHEX(%s), UNHEX(%s), UNHEX(%s), %s, UNHEX(%s), UNHEX(%s), UNHEX(%s), %s, UNHEX(%s), FROM_UNIXTIME(%s), %s, %s, UNHEX(%s), %s)",
		(r['difficulty'],
		r['extraData'][2:],
		r['gasLimit'],
		r['gasUsed'],
		r['hash'][2:],
		r['logsBloom'][2:],
		r['miner'][2:],
		r['mixHash'][2:],
		r['nonce'][2:],
		r['number'],
		r['parentHash'][2:],
		r['receiptsRoot'][2:],
		r['sha3Uncles'][2:],
		r['size'],
		r['stateRoot'][2:],
		tsp,
		r['totalDifficulty'],
		txhashes,
		r['transactionsRoot'][2:],
		unc
		)
		);

		assert r['number'] == expected_num
		assert r['hash'] == blockIdentifier or r['number'] == blockIdentifier

		if isinstance(blockIdentifier, str):
			print("block '%s' number was %d" % (blockIdentifier, r['number']))

		conn.commit()
	except RuntimeError as e:
		conn.rollback()
		raise e
	finally:
		c.close()

def onNewBlockArrive(blockHash):
	global next_expected_block_num

	try:
		fetch_block(blockHash, next_expected_block_num)
		next_expected_block_num += 1
	except:
		exit()

def main():
	global next_expected_block_num

	try:
		print('starting...')
		
		init_web3()
		init_db()
		
		c = conn.cursor();
		
		# どこまでフェッチしたか確認
		c.execute("SELECT MAX(number) AS last_fetched FROM blocks");
		last_fetched = c.fetchone()[0];
		c.close()
		
		# フェッチしたことがないときは-1を設定
		if last_fetched is None:
			last_fetched = -1

		current_block_num = web3.eth.blockNumber
		filter = web3.eth.filter('latest');

		print('last_fetched: %d, current_block_num: %d' % (last_fetched, current_block_num))

		# 前回フェッチしたところ+1から最新まで全てフェッチする
		for i in range(last_fetched + 1, current_block_num + 1):
			fetch_block(i, i)

		# 次にフェッチするはずのブロックは...
		next_expected_block_num = current_block_num + 1
		
		filter.watch(onNewBlockArrive)
		
		print('listening...')
		
		"""
		for i in range(last_fetched + 1):
			fetch_block(i);
		"""
		
		while True:
			sleep(1)
	finally:
		print('closing db connection')
		conn.close()

if __name__ == '__main__':
	main()