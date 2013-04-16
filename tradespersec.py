#!/usr/bin/env python

import datetime, time, urllib2, calendar, json

import pandas as pd
import matplotlib.pyplot as plt
import cPickle

TRADEDATAURL="http://bitcoincharts.com/t/trades.csv?symbol=mtgoxUSD&start=%s&end=%s"
#MTGOXAPIURL="https://data.mtgox.com/api/1/BTCUSD/trades?raw" #&since=1365526800000000"

#date format: "YYYY-MM-DD hh:mm"
DATEFORMAT="%Y-%m-%d %H:%M"

#times are in UTC
STARTTIMESTRING=	"2013-04-09 04:00"
ENDTIMESTRING=		"2013-04-16 08:00"

class MtgoxData():
	MTGOXAPIURL="https://data.mtgox.com/api/1/BTCUSD/trades?raw&since="

	def __init__(self, start, end, log=True):
		self.start = start
		self.end = end
		self.log = log
		self.trades = []

		if end < start and end != -1:
			raise ValueError, "End timestamp must be later than start timestamp"

		if self.log:
			url = self.MTGOXAPIURL
			domain = url[url.find("://")+3:url[url.find("://")+3:].find("/")+url.find("://")+3]
			print "Fetching trade data from %s..." % domain

		currtid = self.start*1000000
		endreached = False
		while True:
			data = self._fetch_data(currtid)
			if (data == '[]'):
				break
			self.trades.extend([MtgoxTrade(a) for a in json.loads(data)])
			currtid = self.trades[-1].tid
			while self.end != -1 and self.trades[-1].gettime() > self.end:
				del self.trades[-1]
				endreached = True
			if endreached:
				break
				


	def _fetch_data(self, starttid):
		url = self.MTGOXAPIURL

		fullurl = url + str(starttid)

		req = urllib2.Request(fullurl)

		res = urllib2.urlopen(req)
		data = res.read()
		res.close()

		return data

class MtgoxTrade():
	def __init__(self, tradedict):
		#format: {"date":1365881612,"price":"105","amount":"0.15","price_int":"10500000","amount_int":"15000000","tid":"1365881612029984",
		#			"price_currency":"USD","item":"BTC","trade_type":"bid","primary":"Y","properties":"market"}
		self.tradedict = tradedict
		self.tid = str(tradedict['tid'])
		self.timestamp = str(tradedict['date'])
		self.price = str(tradedict['price'])
		self.volume = str(tradedict['amount'])

	def __str__(self):
		return "%s %s %s (tid: %s)" % (time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(self.timestamp))), self.price, self.volume, self.tid)

	def getprice(self):
		return float(self.price)

	def getvolume(self):
		return float(self.volume)

	def gettime(self):
		return int(self.timestamp)

	def pandatime(self):
		#pt = pd.datetime.fromtimestamp(int(self.tid[0:-6]))
		#pt.microsecond = int(self.tid[-6:])
		#return pt
		return pd.datetime.utcfromtimestamp(int(self.tid)/1000000.0)

def cache_data_read(domain, starttime, endtime):
	prettyfilename = domain

	if (endtime != 0):
		prettyfilename += "-" + str(starttime) + "-" + str(endtime)
	prettyfilename += ".csv"

	f = open(prettyfilename, "r")
	data = f.read()
	f.close()

	return data

def cache_data_save(data, domain, starttime, endtime):
	prettyfilename = domain

	if (endtime != 0):
		prettyfilename += "-" + str(starttime) + "-" + str(endtime)
	prettyfilename += ".csv"

	f = open(prettyfilename, "w")
	f.write(data)
	f.close()

def get_tradecount(trades):
	prevtime = 0
	#let's try keeping track of all trades indepedently.
	#	if trades appear out of order, the counting will
	#	then still work, ie. if trades with the same timestamp
	#	are interspersed with trades with another timestamp
	tradecount = {}
	for trade in trades:
		if trade.gettime() != prevtime:
			if not trade.gettime() in tradecount:
				tradecount[trade.gettime()] = 0

		tradecount[trade.gettime()] += 1

		prevtime = trade.gettime()

	return tradecount

def pretty_timestamp(timestamp):
	return time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(timestamp)))

def get_tradefrequency(trades):
	#granularity of trades.tid dividided by granularity of HZ and AVG_PRD
	#	1000 = 1000000/1000 (trades.tid is in microseconds, HZ and AVG_PRD in milliseconds)
	TIME_RATIO = 1000
	#sample rate in milliseconds per sample (a value of 1000 will create 1 sample per second)
	HZ=1000
	#average period in milliseconds
	#	for any sample, the value is the average of the number of trades in the period
	#	from sample_time-(AVG_PRD/2) to sample_time+(AVG_PRD/2)
	AVG_PRD=1000
	#note: if HZ is greater than AVG_PRD, samples will be missed in the calculation
	#	we will start averaging the first AVG_PRD milliseconds of data, then move HZ
	#	milliseconds forward and average the AVG_PRD number of milliseconds around this
	#	point in time.

	samples = []
	count = 0
	sample_length = int(trades[-1].tid) - int(trades[0].tid)
	i = 0
	skip_trades = 0

	#progress
	ln = ((sample_length/TIME_RATIO - (AVG_PRD/2)) - AVG_PRD/2) / HZ

	print len(trades)
	print ln
	lastprogress = -1

	for offset in xrange( AVG_PRD/2, sample_length/TIME_RATIO - (AVG_PRD/2), HZ):
		progress = int((float(offset/HZ)/ln*100))
		if (progress != lastprogress):
			print "%d%%" % progress
			lastprogress = progress
		sample_time = int(trades[0].tid) + offset*TIME_RATIO
		#OPTIMIZATION: we KNOW Mt. Gox can't handle more than 1000 TPS (more like 50)
		#	so only loop over the next 1000*AVG_PRD/1000 trades
		#	this speeds up this loop by a factor of at least 1000 when there are
		#	a lot of transactions to check
		for trade in trades[skip_trades:(skip_trades+1000*AVG_PRD/1000)]:
			if int(trade.tid) >= (sample_time-(AVG_PRD*TIME_RATIO/2)):
				if int(trade.tid) <= (sample_time+(AVG_PRD*TIME_RATIO/2)):
					count += 1
					if int(trade.tid) - (sample_time-(AVG_PRD*TIME_RATIO/2)) < HZ*TIME_RATIO:
						skip_trades += 1
				else:
					break
			else:
				print "This shouldn't happen"
		samples.append({'time' : sample_time, 'tps' : float(count)/(AVG_PRD/1000.0)})
		count = 0

	return samples

def tid_to_datetime(tid):
	return pd.datetime.utcfromtimestamp(int(tid)/1000000.0)

def main():
	start=datetime.datetime.strptime(STARTTIMESTRING, DATEFORMAT)
	end=datetime.datetime.strptime(ENDTIMESTRING, DATEFORMAT)
	starttime = int(calendar.timegm(start.timetuple()))
	endtime = int(calendar.timegm(end.timetuple()))

	fileexists = True
	try:
		with open("mtgox.dump"): pass
	except IOError:
		fileexists = False

	if fileexists:
		# to deserialize the object
		with open("mtgox.dump", "rb") as input:
			goxdata = cPickle.load(input) # protocol version is detected
	else:
		try:
			goxdata = MtgoxData(starttime, -1)
		except urllib2.HTTPError, e:
			print "Error:", str(e.code), str(e.reason)
			return
		except urllib2.URLError, e:
			print "Error:", str(e.code), str(e.reason)
			return

	if not fileexists:
		# to serialize the object
		with open("mtgox.dump", "wb") as output:
			cPickle.dump(goxdata, output, cPickle.HIGHEST_PROTOCOL)

	#goxdata.trades = goxdata.trades[:10000]

	print "Calculating trade frequency..."
	trade_freq = get_tradefrequency(goxdata.trades)

	s = pd.Series([float(a.price) for a in goxdata.trades], index=[a.pandatime() for a in goxdata.trades])
	sTPS = pd.Series([a['tps'] for a in trade_freq], index=[tid_to_datetime(a['time']) for a in trade_freq])

	fig = plt.figure()
	plot1 = sTPS.plot(secondary_y=True, label="Transactions per second", color='b', marker='', linestyle='-')
	plt.legend()
	plot2 = s.plot(label="Price", color='r', marker='', linestyle='-')

	plt.show()

	print "Analyzing Mt. Gox data from %s to %s (UTC)..." % ( pretty_timestamp(goxdata.trades[0].gettime()), pretty_timestamp(goxdata.trades[-1].gettime()) )

	tradecount = get_tradecount(goxdata.trades)

	maxtps = 0
	for key in tradecount:
		if tradecount[key] > maxtps:
			maxtps = tradecount[key]

	print "Max. trades per second: %d\n" % maxtps

	#let's see how many times this limit was reached
	limcount = 0
	#this variable defines how many tps less than the maximum tps value we should include in our count
	PLUSMIN = 4
	timestamps = []
	for key in tradecount:
		if tradecount[key] >= maxtps-PLUSMIN:
			limcount += 1
			timestamps.append(key)

	print "Number of times a rate of at least %d trades per second was reached: %d" % (maxtps-PLUSMIN, limcount)

	print "This happened the following times:"

	for timestamp in sorted(timestamps):
		print "%s UTC (%d trades per second)" % (pretty_timestamp(timestamp), tradecount[timestamp])

	print "Average trades per second for the entire period specified: %.2f" % ( float(len(goxdata.trades)) / (goxdata.trades[-1].gettime()-goxdata.trades[0].gettime()) )	

if __name__ == "__main__":
	main()
