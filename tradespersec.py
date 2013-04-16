#!/usr/bin/env python

import datetime, time, urllib2, calendar, json

TRADEDATAURL="http://bitcoincharts.com/t/trades.csv?symbol=mtgoxUSD&start=%s&end=%s"
#MTGOXAPIURL="https://data.mtgox.com/api/1/BTCUSD/trades?raw" #&since=1365526800000000"

#date format: "YYYY-MM-DD hh:mm"
DATEFORMAT="%Y-%m-%d %H:%M"

#times are in UTC
#STARTTIMESTRING=	"2013-04-09 17:00"
STARTTIMESTRING=	"2013-04-16 04:00"
ENDTIMESTRING=		"2013-04-16 07:00"

class MtgoxData():
	MTGOXAPIURL="https://data.mtgox.com/api/1/BTCUSD/trades?raw&since="

	def __init__(self, start, end, log=True):
		self.start = start
		self.end = end
		self.log = log
		self.trades = []

		if end < start:
			raise ValueError, "End timestamp must be later than start timestamp"

		if self.log:
			url = self.MTGOXAPIURL
			domain = url[url.find("://")+3:url[url.find("://")+3:].find("/")+url.find("://")+3]
			print "Fetching trade data from %s..." % domain

		currtid = self.start*1000000
		while True:
			data = self._fetch_data(currtid)
			if (data == '[]'):
				break
			self.trades.extend([MtgoxTrade(a) for a in json.loads(data)])
			currtid = self.trades[-1].tid


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
	#domain = url[url.find("://")+3:url[url.find("://")+3:].find("/")+url.find("://")+3]
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


def main():
	start=datetime.datetime.strptime(STARTTIMESTRING, DATEFORMAT)
	end=datetime.datetime.strptime(ENDTIMESTRING, DATEFORMAT)
	starttime = int(calendar.timegm(start.timetuple()))
	endtime = int(calendar.timegm(end.timetuple()))

	##CHECK MT. GOX DATA
	try:
		goxdata = MtgoxData(starttime, endtime)
	except urllib2.HTTPError, e:
		print "Error:", str(e.code), str(e.reason)
		return
	except urllib2.URLError, e:
		print "Error:", str(e.code), str(e.reason)
		return

	tradecount = get_tradecount(goxdata.trades)

	print "Analyzing Mt. Gox data from %s to %s (UTC)..." % ( pretty_timestamp(goxdata.trades[0].gettime()), pretty_timestamp(goxdata.trades[-1].gettime()) )

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

	#print "Average trades per second for the entire period specified: %.2f" % (float(len(data.split("\n")))/(endtime-starttime))	

if __name__ == "__main__":
    main()
