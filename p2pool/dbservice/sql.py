
import logging
import sys
import re
import string
from Queue import Queue
import threading
import traceback
import config
import urllib, urllib2, json
from time import time

class shareLogFormatter:
	_re_x = re.compile(r'^\s*(\w+)\s*(?:\(\s*(.*?)\s*\))?\s*$')
	
	def __init__(self, *a, **ka):
		self._p = self.parse(*a, **ka)
	
	# NOTE: This only works for psf='%s' (default)
	def formatShare(self, *a, **ka):
		(stmt, params) = self.applyToShare(*a, **ka)
		return stmt % params
	
	def applyToShare(self, share):
		(stmt, stmtf) = self._p
		params = []
		for f in stmtf:
			params.append(f(share))
		params = tuple(params)
		return (stmt, params)
	
	@classmethod
	def parse(self, stmt, psf = '%s'):
		fmt = string.Formatter()
		pstmt = tuple(fmt.parse(stmt))
		
		stmt = ''
		fmt = []
		for (lit, field, fmtspec, conv) in pstmt:
			stmt += lit
			if not field:
				continue
			f = self.get_field(field)
			fmt.append(f)
			stmt += psf
		fmt = tuple(fmt)
		return (stmt, fmt)
	
	@classmethod
	def get_field(self, field):
		m = self._re_x.match(field)
		if m:
			if m.group(2) is None:
				# identifier
				return lambda s: s.get(field, None)
			else:
				# function
				fn = m.group(1)
				sf = self.get_field(m.group(2))
				gfm = 'get_field_%s' % (fn,)
				if hasattr(self, gfm):
					return getattr(self, gfm)(sf)
				f = eval(fn)
				return self._get_field_auto(f, sf)
		raise ValueError('Failed to parse field: %s' % (field,))
	
	@classmethod
	def _get_field_auto(self, f, subfunc):
		return lambda s: f(subfunc(s))
	
	@classmethod
	def get_field_not(self, subfunc):
		return lambda s: not subfunc(s)
	
	@classmethod
	def get_field_Q(self, subfunc):
		return lambda s: subfunc(s) or '?'
	
	@classmethod
	def get_field_dash(self, subfunc):
		return lambda s: subfunc(s) or '-'

class sql:
	_psf = {
		'qmark': '?',
		'format': '%s',
		'pyformat': '%s',
	}
	
	def __init__(self, **ka):
		self.opts = ka
		dbe = ka['engine']
		self.exceptions = []
		self.threadsafe = False
		getattr(self, 'setup_%s' % (dbe,))()
		if self.threadsafe:
			self._logShareF = self._doInsert
			self.stop = self._shutdown
			self._connect()
		else:
			self._queue = Queue()
			self._refreshTime = time()
			self._logShareF = self._queue.put
			threading.Thread(target=self._threadEvent).start()
	
	def _doInsert(self, o):
		(stmt, params) = o
		dbc = self.db.cursor()
		try:
			dbc.execute(stmt, params)
		except BaseException as e:
			logging.error('Error inserting data: %s%s' % ((stmt, params), traceback.format_exc()))
			self.exceptions.append((stmt, params, e))
			return
		self.db.commit()
	
	def _thread(self):
		self._connect()
		while True:
			try:
				o = self._queue.get()
				if o is None:
					# Shutdown logger
					break
				self._doInsert(o)
				
				timenow = time()
				if timenow-self._refreshTime > 60:
					self._refreshTime = timenow
					logging.info('Pool sql queue len: %d', self._queue.qsize())
			except:
				_logger.critical(traceback.format_exc())
		self._shutdown()

	def _doInsertEvent(self, dbc, o):
		(stmt, params) = o
		try:
			dbc.execute(stmt, params)
		except BaseException as e:
			logging.error('Error inserting data: %s%s' % ((stmt, params), traceback.format_exc()))
			self.exceptions.append((stmt, params, e))
			return
	
	def _threadEvent(self):
		starttime = None
		self._connect()
		while True:
			try:
				dbc = self.db.cursor()
				size = self._queue.qsize()
				starttime = time()
				if starttime-self._refreshTime > 60:
					self._refreshTime = starttime
					logging.info('Pool sql queue len: %d', size)
				for i in range(1, size):
					o = self._queue.get()
					if o is None:
						logging.error('Pool sql queue get none')
						continue
					self._doInsertEvent(dbc, o)
				self.db.commit()
				timenow = time()
				if timenow-starttime > 20:
					logging.info('Pool sql queue len: %d time: %d', size, timenow-starttime)
			except:
				_logger.critical(traceback.format_exc())
		self._shutdown()
	
	def setup_mysql(self):
		import pymysql
		dbopts = self.opts.get('dbopts', {})
		if 'passwd' not in dbopts and 'password' in dbopts:
			dbopts['passwd'] = dbopts['password']
			del dbopts['password']
		self.modsetup(pymysql)
		
	def setup_postgres(self):
		import psycopg2
		dbopts = self.opts.get('dbopts', {})
		if 'database' not in dbopts and 'db' in dbopts:
			dbopts['database'] = dbopts['db']
			del dbopts['db']
		if 'password' not in dbopts and 'passwd' in dbopts:
			dbopts['password'] = dbopts['passwd']
			del dbopts['passwd']
		self.modsetup(psycopg2)
	
	def modsetup(self, mod):
		self._mod = mod
		psf = self._psf[mod.paramstyle]
		stmt = 'insert into t_user_share(user_worker_id,user_oid,remote_host,block_height,network_diff,payment_method,min_diff,speed,share1_count,stale1_count,dupe1_count,other1_count,diff_max,time_space)values({username},{userOid},{remoteHost},{blockHeight},{networkDiff},{paymentMethod},{minDiff},{speed},{share1Count},{stale1Count},{dupe1Count},{other1Count},{diffMax},{timeSpace})'
		self.pstmt_insertshare = shareLogFormatter(stmt, psf)
		stmt = 'insert into t_pool_block (block_height,tx_hash,bits,solution,block_value,block_diff,network_diff,user_worker_id,user_oid,me_flag)values({blockHeight},{txHash},{bits},{solution},{blockValue},{blockDiff},{networkDiff},{userWorkerId},{userOid},{meFlag})'
		self.pstmt_insertpoolblock = shareLogFormatter(stmt, psf)
		stmt = 'insert into t_net_block (block_height,tx_hash,bits,block_value,network_diff)values({blockHeight},{txHash},{bits},{blockValue},{networkDiff})'
		self.pstmt_insertnetblock = shareLogFormatter(stmt, psf)
		stmt = 'insert into t_pool_status (speed,status_diff,network_diff,active_worker,share_count,share1_count,stalepblk_count,stalepblk1_count,badpblk_count,badpblk1_count,baddfbits_count,baddfbits1_count,badver_count,badver1_count,unuser_count,unuser1_count,unwork_count,unwork1_count,dupe_count,dupe1_count,hw_count,hw1_count,highhash_count,highhash1_count,stalework_count,stalework1_count,timeold_count,timeold1_count,timenew_count,timenew1_count,badcbprefix_count,badcbprefix1_count,badcbflag_count,badcbflag1_count,badcblen_count,badcblen1_count,badtxnmr_count,badtxnmr1_count,badtxns_count,badtxns1_count,other_count,other1_count,block_height,diff_min,diff_max,diff_avg,share_diff1,share_count1,share1_count1,share_diff2,share_count2,share1_count2,share_diff3,share_count3,share1_count3,share_diff4,share_count4,share1_count4,time_space)values({speed},{statusDiff},{networkDiff},{activeWorker},{shareCount},{share1Count},{stalepblkCount},{stalepblk1Count},{badpblkCount},{badpblk1Count},{baddfbitsCount},{baddfbits1Count},{badverCount},{badver1Count},{unuserCount},{unuser1Count},{unworkCount},{unwork1Count},{dupeCount},{dupe1Count},{hwCount},{hw1Count},{highhashCount},{highhash1Count},{staleworkCount},{stalework1Count},{timeoldCount},{timeold1Count},{timenewCount},{timenew1Count},{badcbprefixCount},{badcbprefix1Count},{badcbflagCount},{badcbflag1Count},{badcblenCount},{badcblen1Count},{badtxnmrCount},{badtxnmr1Count},{badtxnsCount},{badtxns1Count},{otherCount},{other1Count},{blockHeight},{diffMin},{diffMax},{diffAvg},{shareDiff1},{shareCount1},{share1Count1},{shareDiff2},{shareCount2},{share1Count2},{shareDiff3},{shareCount3},{share1Count3},{shareDiff4},{shareCount4},{share1Count4},{timeSpace})'
		self.pstmt_insertpoolstatus = shareLogFormatter(stmt, psf)
	
	def _connect(self):
		self.db = self._mod.connect(**self.opts.get('dbopts', {}))
	
	def logShare(self, share):
		o = self.pstmt_insertshare.applyToShare(share)
		self._logShareF(o)
		
	def logPoolBlock(self, poolBlock):
		o = self.pstmt_insertpoolblock.applyToShare(poolBlock)
		self._logShareF(o)
	
	def logNetBlock(self, netBlock):
		o = self.pstmt_insertnetblock.applyToShare(netBlock)
		self._logShareF(o)
	
	def logPoolStatus(self, poolStatus):
		o = self.pstmt_insertpoolstatus.applyToShare(poolStatus)
		self._logShareF(o)
		
	def checkUsername(self, username):
		userId = ''
		workerId = ''
		index = username.index('_')
		length = len(username)
		if index == -1:
			userId = username
			workerId = ''
		elif index == length-1:
			userId = username[0:index]
			workerId = ''
		else:
			userId = username[0:index]
			workerId = username[index+1:length]
			
		if userId == '':
			return False
		p = re.compile('^[A-Za-z0-9@#.|+-]+$')
		if not p.match(userId):
			return False
		
		if workerId == '':
			return False
		p = re.compile('^\\w+$')
		if not p.match(workerId):
			return False
		return True
	
	def dbOpen(self):
		conn = None
		try:
			conn = self._mod.connect(**self.opts.get('dbopts', {}))
		except:
			logging.error('db open unknown error %s', sys.exc_info())
			conn = None
		return conn
	
	def dbClose(self, conn):
		try:
			if conn is not None:
				conn.close()
		except:
			logging.error('db close unknown error %s', sys.exc_info())
		
	def getUpdateCount(self, conn, sql):
		cursor = conn.cursor()
		cursor.execute(sql)
		result = cursor.fetchone()
		return result[0]
	
	def getWorkerUpdateData(self, conn, start, count):
		resultsData = {}
		cursor = conn.cursor()
		cursor.execute('select id, user_worker_id, payment_method, min_diff, diff_type from t_user_worker where update_flag = \'1\' limit %d offset %d' % (count, start))
		results = cursor.fetchall()
		for result in results:
			id = result[0]
			userWorkerId = result[1]
			paymentMethod = result[2]
			minDiff = result[3]
			diffType = result[4]
			
			if diffType == '0':
				minDiff = 0
			
			resultsData[id] = {
				'username': userWorkerId,
				'paymentMethod': paymentMethod,
				'minDiff': minDiff
			}
		cursor.close()
		return resultsData
	
	def sqlExecute(self, conn, sql):
		cursor = conn.cursor()
		cursor.execute(sql)
		conn.commit()
		cursor.close()

	def checkAuthentication(self, user, password, ip = ''):
		postdata = {
			'username': user,
			'password': password,
			'ip': ip,
		};
		url = config.NodeService['authentication'] + '?m=au'
		reqdata = urllib.urlencode(postdata)
		reqdata = reqdata.encode('utf_8')
		response = urllib2.urlopen(url, reqdata)
		resdata = response.read()
		resdata = resdata.decode('utf_8')
		resdata = json.loads(resdata)
		
		if resdata['IsError']:
			#logging.error('checkAuthentication %s %s error: %s', ip, user, resdata['Message'])
			return None
		
		if resdata['Data']['diffType'] == '0':
			userWorker = {
				'userOid': resdata['Data']['userOid'],
				'userId': resdata['Data']['userId'],
				'workerId': resdata['Data']['workerId'],
				'userWorkerId': resdata['Data']['userWorkerId'],
				'paymentMethod': resdata['Data']['paymentMethod'],
				'minDiff': 0,
				'updateFlag': '0'
			}
		else:
			userWorker = {
				'userOid': resdata['Data']['userOid'],
				'userId': resdata['Data']['userId'],
				'workerId': resdata['Data']['workerId'],
				'userWorkerId': resdata['Data']['userWorkerId'],
				'paymentMethod': resdata['Data']['paymentMethod'],
				'minDiff': resdata['Data']['minDiff'],
				'updateFlag': '0'
			}
		return userWorker
	
	def stop(self):
		# NOTE: this is replaced with _shutdown directly for threadsafe objects
		self._queue.put(None)
	
	def _shutdown(self):
		self.db.close()
