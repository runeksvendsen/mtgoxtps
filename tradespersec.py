#!/usr/bin/env python

import datetime, time, urllib2, calendar

TRADEDATAURL="http://bitcoincharts.com/t/trades.csv?symbol=mtgoxUSD&start=%s&end=%s"

#date format: "YYYY-MM-DD hh:mm"
DATEFORMAT="%Y-%m-%d %H:%M"

#times are in UTC
STARTTIMESTRING=	"2013-04-09 17:00"
ENDTIMESTRING=		"2013-04-14 17:00"

class Trade():
	def __init__(self, tradestring):
		#format: "1365670789,163.400000000000,5.867343130000"
		self.tradestring = tradestring
		self.timestamp = tradestring.split(",")[0]
		self.price = tradestring.split(",")[1]
		self.volume = tradestring.split(",")[2]

	def __str__(self):
		return "%s %s %s" % (time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int('1365670789'))), self.price, self.volume)

	def getprice(self):
		return float(self.price)

	def getvolume(self):
		return float(self.volume)

	def gettime(self):
		return int(self.timestamp)

def main():
	start=datetime.datetime.strptime(STARTTIMESTRING, DATEFORMAT)
	end=datetime.datetime.strptime(ENDTIMESTRING, DATEFORMAT)
	starttime = int(calendar.timegm(start.timetuple()))
	endtime = int(calendar.timegm(end.timetuple()))

	#check whether we've already downloaded the data, to avoid hammering
	#	btccharts' API too much
	fileexists = True
	try:
		with open("trades.csv"): pass
	except IOError:
		fileexists = False

	if not fileexists:
		url = TRADEDATAURL % (starttime, endtime)

		req = urllib2.Request(url)

		print "Fetching trade data from bitcoincharts.com..."
		try:
			res = urllib2.urlopen(req)
		except urllib2.HTTPError, e:
			print "Error:", str(e.code), str(e.reason)
			return
		except urllib2.URLError, e:
			print "Error:", str(e.code), str(e.reason)
			return
		except httplib.HTTPException, e:
			print('HTTPException')

		data = res.read()

		f = open("trades.csv", "w")
		f.write(data)
		f.close()
	else:
		f = open("trades.csv", "r")
		data = f.read()
		f.close()

	print "Analyzing trade data for the period %s to %s (UTC)..." % (STARTTIMESTRING, ENDTIMESTRING)

	prevtime = 0
	#let's try keeping track of all trades indepedently.
	#	if trades appear out of order, the counting will
	#	then still work, ie. if trades with the same timestamp
	#	are interspersed with trades with another timestamp
	tradecount = {}
	for tradestr in data.split("\n"):
		trade = Trade(tradestr)

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
