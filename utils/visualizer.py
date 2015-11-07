import matplotlib.pyplot as plt
import numpy as np
import requests
import json


x = np.linspace(0, 20, 100)
y = np.linspace(0, 1000, 100)
z = np.linspace(0, 5000, 100)
# You probably won't need this if you're embedding things in a tkinter plot...
plt.ion()

fig1 = plt.figure()
ax1 = fig1.add_subplot(111)
fig2 = plt.figure()
ax2 = fig2.add_subplot(111)
line1, = ax1.plot(x, y, 'r-') # Returns a tuple of line objects, thus the comma
line2, = ax2.plot(x, z, 'b-') # Returns a tuple of line objects, thus the comma
#
# for phase in np.linspace(0, 10, 500):
#     line1.set_ydata(np.sin(x + phase))
#     line2.set_ydata(np.sin(x + phase * 2))
#     fig1.canvas.draw()
#     fig2.canvas.draw()

# rs = []
# for i in xrange(100):
#     r = requests.get('http://52.8.82.157:8888/bids')
#     rs.append(json.loads(r.content))

fraud_num = 0

import time

started = time.time()
while True:
    r = requests.get('http://52.8.82.157:8888/bids')
    # response = rs.pop(0)
    # rs.append(json.loads(r.content))
    response = json.loads(r.content)
    line1.set_ydata(len(response.get('total_bids', [])))
    fraud_num += len(response.get('anomalies', []))
    line2.set_ydata(fraud_num)
    fig1.canvas.draw()
    fig2.canvas.draw()
    # if (int(time.time()) - int(started)) > 10:
    #     fraud_num = 0
