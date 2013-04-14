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
	def __init__(self, tradestring):
		#format: {"date":1365881612,"price":"105","amount":"0.15","price_int":"10500000","amount_int":"15000000","tid":"1365881612029984",
		#			"price_currency":"USD","item":"BTC","trade_type":"bid","primary":"Y","properties":"market"}
		self.tradestring = tradestring
		data = json.loads(tradestring)
		self.timestamp = str(data['date'])
		self.price = str(data['price'])
		self.volume = str(data['amount'])

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
	prettyfilename = domain + ".csv"

	fileexists = True
	try:
		with open(prettyfilename): pass
	except IOError:
		fileexists = False

	if not fileexists:
		if (starttime != 0 and endtime != 0):
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


def main():
	start=datetime.datetime.strptime(STARTTIMESTRING, DATEFORMAT)
	end=datetime.datetime.strptime(ENDTIMESTRING, DATEFORMAT)
	starttime = int(calendar.timegm(start.timetuple()))
	endtime = int(calendar.timegm(end.timetuple()))

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

	print "Analyzing trade data for the period %s to %s (UTC)..." % (STARTTIMESTRING, ENDTIMESTRING)

	prevtime = 0
	#let's try keeping track of all trades indepedently.
	#	if trades appear out of order, the counting will
	#	then still work, ie. if trades with the same timestamp
	#	are interspersed with trades with another timestamp
	tradecount = {}
	for tradestr in data.split("\n"):
		trade = BitcoinchartsTrade(tradestr)

		if trade.gettime() != prevtime:
			if not trade.gettime() in tradecount:
				tradecount[trade.gettime()] = 0

		tradecount[trade.gettime()] += 1

		prevtime = trade.gettime()

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
		print "%s UTC (%d trades per second)" % (time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(timestamp))), tradecount[timestamp])

	print "Average trades per second for the entire period specified: %.2f" % (float(len(data.split("\n")))/(endtime-starttime))	

if __name__ == "__main__":
    main()
