from __future__ import division
from bokeh.layouts import column, gridplot
from bokeh.models import ColumnDataSource
from bokeh.plotting import curdoc, figure
from functools import partial
from threading import Thread
from math import pi
from random import shuffle
from pytz import timezone
import datetime as dt
from collections import defaultdict
from random import uniform

from tornado import gen

class FractalSynthesiser():
    def __init__ (self, interval, history, tz):
        self.interval = interval
        self.history = history
        self.zone = timezone(tz)
        
        #Store value to permit easy resumption of synthesise() across calls
        self.resume_from = (0, 0)        
        #Time & date now; need to use utc to permit later conversion and timezone setting
        self.now = dt.datetime.utcnow()
        print(self.now)
        #Convert time to seconds
        self.secondsElapsedToday = self.now.hour*3600 + self.now.minute*60 + self.now.second
        #Get the modulo to permit alignment of time interval
        self.modulo = self.secondsElapsedToday % self.interval
        #Align the datum & set to the interval in which current time is contained
        self.datum = dt.datetime(self.now.year, self.now.month, self.now.day, 0, 0, 0, 0) + dt.timedelta(seconds=self.secondsElapsedToday - self.modulo + self.interval)
        print(self.datum)

    #Create a load of fractally synthesised tick data
    #Source; http://stackoverflow.com/questions/25067096/how-to-generate-a-fractal-graph-of-a-market-in-python
    def synthesise(self, depth, graph, start, end, turns):
        #Set stores tuples: (datetime, value)
        graph.add((dt.datetime.utcfromtimestamp(start[0]).replace(tzinfo=self.zone),start[1]))
        graph.add((dt.datetime.utcfromtimestamp(end[0]).replace(tzinfo=self.zone), end[1]))   
        
        if depth > 0:   
            # unpack input values
            fromtime, fromvalue = start
            totime, tovalue = end
            # calculate differences between points
            diffs = []
            last_time, last_val = fromtime, fromvalue
            for t, v in turns:
                new_time = fromtime + (totime - fromtime) * t
                new_val = fromvalue + (tovalue - fromvalue) * v
                diffs.append((new_time - last_time, new_val - last_val))
                last_time, last_val = new_time, new_val

            # add 'brownian motion' by reordering the segments
            shuffle(diffs)

            # calculate actual intermediate points and recurse
            last = start
            for segment in diffs:
                p = last[0] + segment[0], last[1] + segment[1]
                self.synthesise(depth - 1, graph, last, p, turns)
                last = p
            self.synthesise(depth - 1, graph, last, end, turns)    
            #Store
            self.resume_from = end


class Chart():
    def __init__ (self, document, datum, tz, interval, history, ticks):
        
        #Instance variables stored across method calls for candlestickmaker function
        self.count = 0
        #Rollover; parameter of patch(), maximum candles on the chart.
        self.rollover = 105
        #Index; indicates which element to update in patch()
        self.index = 0
        self.op, self.hi, self.lo, self.cl = 0, 0, 0, 0
        self.time_delta = dt.timedelta(seconds=interval)
        self.begin_dt = datum.replace(tzinfo=timezone(tz)) - (self.time_delta * history)
        self.candle_close_time = self.begin_dt + self.time_delta
        
        self.candle_data_list = []
        self.candlestickmaker(ticks)
        
        #Prepare source; pass dictionary of key:list        
        #self.candles_list = list(self.candlestickmaker(ticks))
        self.candle_data_dictionary = defaultdict(list)
        for d, o, h ,l, c, z in self.candle_data_list:
            self.candle_data_dictionary['d'].append(d)
            self.candle_data_dictionary['o'].append(o)
            self.candle_data_dictionary['h'].append(h)
            self.candle_data_dictionary['l'].append(l)
            self.candle_data_dictionary['c'].append(c)    
            self.candle_data_dictionary['clr'].append(z)            
                
        self.source = ColumnDataSource(self.candle_data_dictionary)
        
        self.doc = document        
        
        #Candle width
        self.p = figure(webgl=True) # for the plotting API
        self.w = interval * 500                
        self.p = figure(plot_height=500, tools="xpan,xwheel_zoom,xbox_zoom,reset", x_axis_type="datetime", y_axis_location="right", title = "Synthesised Fractal Candlesticks")
        self.p.xaxis.major_label_orientation = pi/4
        self.p.grid.grid_line_alpha=0.3
        self.p.background_fill_color = "#000000"        
        
        #Wick
        self.p.segment(x0='d', y0='l', x1='d', y1='h', line_width=2, color="#8D8C8C", source=self.source)
        #Body
        self.p.segment(x0='d', y0='o', x1='d', y1='c', line_width=8, color='clr', source=self.source)
               
        self.doc.add_root(column(gridplot([[self.p]], toolbar_location="left", plot_width=1000)))
        self.doc.title = "OHLC_RT"
        
    @gen.coroutine
    def candlestickmaker(self, tick_data, realtime=False):
        #Fills a list of candlesticks for use in the Chart constructor, also calls stream and patch where appropriate for chart updates.  Uses instance variables as position holders.
        colour = "#09ff00"
        lght = len(tick_data)
        monitor_consumption = 0
        for (dtval, val) in tick_data:
            monitor_consumption +=1
            #if realtime == True:
                #print(str(dtval) + " candle close: " + str(self.candle_close_time) + " price: " + str(val))            
            if (self.candle_close_time < dtval) or (realtime == False and monitor_consumption == lght):
                #if self.op != 0:
                #store the completed candle
                if realtime == False:
                    colour = "#09ff00" if self.op < self.cl else "#ff0000"
                    print('Store completed candle: realtime: ' + str(realtime) + ' cnt: ' + str(self.count) + ' cct: ' + str(self.candle_close_time) + ' o: ' + str(self.op) + ' h: ' + str(self.hi) + ' l: ' + str(self.lo) + ' c: ' + str(self.cl))
                    self.candle_data_list.append((self.candle_close_time, self.op, self.hi, self.lo, self.cl, colour))#accumulate candlestick history
                    self.count += 1                        
                    if monitor_consumption != lght:
                        #increment to the next candle
                        self.candle_close_time += self.time_delta
                        #Reset
                        self.op, self.hi, self.lo, self.cl = 0, 0, 0, 0    
                else:
                    print('Stream completed candle: realtime: ' + str(realtime) + ' cnt: ' + str(self.count) + ' cct: ' + str(self.candle_close_time) + ' o: ' + str(self.op) + ' h: ' + str(self.hi) + ' l: ' + str(self.lo) + ' c: ' + str(self.cl))
                    colour = "#09ff00" if self.op < self.cl else "#ff0000"
                    self.source.stream({'d':[self.candle_close_time], 'o':[self.op], 'h':[self.hi], 'l':[self.lo], 'c':[self.cl], 'clr':[colour]}, self.rollover)#stream completed candle
                    self.count += 1
                    #increment to the next candle
                    self.candle_close_time += self.time_delta
                    #Reset
                    self.op, self.hi, self.lo, self.cl = 0, 0, 0, 0            
            
            if dtval <= self.candle_close_time and self.op==0:
                #set initial values
                self.op, self.hi, self.lo, self.cl = val, val, val, val
                #print('Set initial values: realtime: ' + str(realtime) + ' cnt: ' + str(self.count) + ' cct: ' + str(self.candle_close_time) + ' o: ' + str(self.op) + ' h: ' + str(self.hi) + ' l: ' + str(self.lo) + ' c: ' + str(self.cl))
            elif dtval <= self.candle_close_time and self.op!=0:
                #update values as appropriate                
                self.hi = val if val > self.hi else self.hi
                self.lo = val if val < self.lo else self.lo
                self.cl = val
                colour = "#09ff00" if self.op < self.cl else "#ff0000"
                if realtime == True
                    #Determine index, accounting for rollover
                    self.index = min(self.rollover-1, self.count-1)
                    #print('Patch candle: realtime: ' + str(realtime) + ' index: ' + str(self.count-1) + ' cct: ' + str(self.candle_close_time) + ' o: ' + str(self.op) + ' h: ' + str(self.hi) + ' l: ' + str(self.lo) + ' c: ' + str(self.cl))
                    self.source.patch({'d':[(self.index, self.candle_close_time)], 'o':[(self.index, self.op)], 'h':[(self.index, self.hi)], 'l':[(self.index, self.lo)], 'c':[(self.index, self.cl)], 'clr':[(self.index, colour)]})#patch updated candle
 
#Helper method    
def seconds_since_epoch(dt_value):
        return (dt_value - dt.datetime.utcfromtimestamp(0)).total_seconds()

#Simulate updates arriving from an external data feed
def financial_market(chart, data):
    cnt = 0
    length = len(data)    
    print('There are: ' + str(length) + ' future tick data points.  Extending from: ' + str(data[0][0]) + ' until: ' + str(data[-1][0]))
    while cnt < length:
        #print(str(data[cnt][0]) + ' <= ' + str(dt.datetime.utcnow().replace(tzinfo=timezone('Europe/London'))))
        if data[cnt][0] <= dt.datetime.utcnow().replace(tzinfo=timezone('Europe/London')):
            chart.doc.add_next_tick_callback(partial(chart.candlestickmaker, [data[cnt]], realtime=True,))
            cnt+=1            
        #run flat out; there can be many thousands of ticks to process, would not keep up if sleep() was used    
        
'''

    Instantiate objects, prepare data and run the simulation
    
'''
        
#Candlestick; interval in seconds, history as number of candlesticks
interval = 10
history = 100

#Set is used to gather synthesised tick data from the recursive synthesise() function
grph = set()

#Depth; a parameter of synthesise()
dpth = 10

#Instantiate a FractalSynthesiser
fracSyn = FractalSynthesiser(interval, history, 'Europe/London')

#Generate the tick data; pass time as 'seconds since epoch' to permit arithmetic operators to function on the values.  Generates synthetic historical and future data.
print("Fractally Synthesising")
fracSyn.synthesise(dpth,
                     grph,
                     (seconds_since_epoch(fracSyn.datum) - (interval * history), uniform(50, 100)),#data history
                     (seconds_since_epoch(fracSyn.datum) + (interval * history), uniform(50, 100)),#future data for streaming and patching
                     [(1/9, 2/3), (5/9, 1/3)])

print("Finished Synthesising")

#Prepare data; sort into order and convert to a list
sorted_graph = sorted(grph)
tick_data = list(sorted_graph)        

#Filter out the historical data
datum_tz_aware = fracSyn.datum.replace(tzinfo=timezone('Europe/London'))
historical_data = list(filter(lambda x: x[0] <= datum_tz_aware, tick_data))

#Filter out the future data
future_data = list(filter(lambda x: x[0] > datum_tz_aware, tick_data))

# # This is important! Save curdoc() to make sure all threads
# # see the same document.
dcmnt = curdoc()

#Instantiate the OHLC chart and plot the data history
chrt = Chart(dcmnt, fracSyn.datum, 'Europe/London', interval, history, historical_data)

thread = Thread(target=financial_market, args=(chrt, future_data,))
thread.start()
