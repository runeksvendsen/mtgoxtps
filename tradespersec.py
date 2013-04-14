#!/usr/bin/env python

import datetime, time, urllib2, calendar, json

TRADEDATAURL="http://bitcoincharts.com/t/trades.csv?symbol=mtgoxUSD&start=%s&end=%s"
MTGOXAPIURL="https://data.mtgox.com/api/1/BTCUSD/trades?raw"

#date format: "YYYY-MM-DD hh:mm"
DATEFORMAT="%Y-%m-%d %H:%M"

#times are in UTC
STARTTIMESTRING=	"2013-04-09 17:00"
ENDTIMESTRING=		"2013-04-14 17:00"

class BitcoinchartsTrade():
	def __init__(self, tradestring):
		#format: "1365670789,163.400000000000,5.867343130000"
		self.tradestring = tradestring
		split = tradestring.split(",")
		self.timestamp = split[0]
		self.price = split[1]
		self.volume = split[2]


	def __str__(self):
		return "%s %s %s" % (time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(self.timestamp))), self.price, self.volume)

	def getprice(self):
		return float(self.price)

	def getvolume(self):
		return float(self.volume)

	def gettime(self):
		return int(self.timestamp)

class MtgoxTrade():
	def __init__(self, tradedict):
		#format: {"date":1365881612,"price":"105","amount":"0.15","price_int":"10500000","amount_int":"15000000","tid":"1365881612029984",
		#			"price_currency":"USD","item":"BTC","trade_type":"bid","primary":"Y","properties":"market"}
		self.tradedict = tradedict
		self.timestamp = str(tradedict['date'])
		self.price = str(tradedict['price'])
		self.volume = str(tradedict['amount'])

	def __str__(self):
		return "%s %s %s" % (time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(self.timestamp))), self.price, self.volume)

	def getprice(self):
		return float(self.price)

	def getvolume(self):
		return float(self.volume)

	def gettime(self):
		return int(self.timestamp)

def get_trade_data(url, starttime, endtime):
	#check whether we've already downloaded the data, to avoid hammering
	#	the API servers too much
	domain = url[url.find("://")+3:url[url.find("://")+3:].find("/")+url.find("://")+3]

	prettyfilename = domain
	if (endtime != 0):
		prettyfilename += "-" + str(starttime) + "-" + str(endtime)
	prettyfilename += ".csv"

	fileexists = True
	try:
		with open(prettyfilename): pass
	except IOError:
		fileexists = False

	if not fileexists:
		if (endtime != 0):
			url = url % (starttime, endtime)

		req = urllib2.Request(url)

		print "Fetching trade data from %s..." % domain

		res = urllib2.urlopen(req)

		data = res.read()

		f = open(prettyfilename, "w")
		f.write(data)
		f.close()
	else:
		f = open(prettyfilename, "r")
		data = f.read()
		f.close()

	return data

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
	mtgoxdata = get_trade_data(MTGOXAPIURL, 0, 0)
	trades = [MtgoxTrade(a) for a in json.loads(mtgoxdata)]
	tradecount = get_tradecount(trades)

	print "Analyzing Mt. Gox data from %s to %s..." % ( pretty_timestamp(trades[0].gettime()), pretty_timestamp(trades[-1].gettime()) )

	maxtps = 0
	for key in tradecount:
		if tradecount[key] > maxtps:
			maxtps = tradecount[key]

	print "Max. trades per second (Mt. Gox data): %d\n" % maxtps

	#TEST. use the same time frame as Mt. Gox' API returns
	#	helps to see if bitcoincharts.com and Mt. Gox agree.
	starttime = trades[0].gettime()
	endtime = trades[-1].gettime()

	##CHECK MT. GOX DATA END

	try:
		data = get_trade_data(TRADEDATAURL, starttime, endtime)
	except urllib2.HTTPError, e:
		print "Error:", str(e.code), str(e.reason)
		return
	except urllib2.URLError, e:
		print "Error:", str(e.code), str(e.reason)
		return
	except httplib.HTTPException, e:
		print('HTTPException')
		return

	print "Analyzing trade data for the period %s to %s (UTC)..." % (pretty_timestamp(starttime), pretty_timestamp(endtime))

	tradecount = get_tradecount([BitcoinchartsTrade(a) for a in data.split("\n")])

	maxtps = 0
	for key in tradecount:
		if tradecount[key] > maxtps:
			maxtps = tradecount[key]

	print "Max. trades per second: %d" % maxtps

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

	print "Average trades per second for the entire period specified: %.2f" % (float(len(data.split("\n")))/(endtime-starttime))	

if __name__ == "__main__":
    main()
