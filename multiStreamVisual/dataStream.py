import random
from itertools import count
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import statistics


plt.style.use('fivethirtyeight')

x_vals = []

index = count()

bigList = []
stdDev = []

dataLabels = []
stdDevLabels = []

for i in range(2):
    bigList.append([])
    dataLabels.append("Stream: " + str(i))
    stdDevLabels.append("StdDev Stream: " + str(i))
    stdDev.append([])




def animate(i):
    for i in range(len(bigList)):
        bigList[i].append(random.randint(0,10))
        plt.plot(bigList[i], label=dataLabels[i])
        if len(bigList[i]) > 1:
            stdDev[i].append(statistics.stdev(bigList[i]))
            plt.plot(stdDev[i], label=stdDevLabels[i])
        else:
            stdDev[i].append(0)
    

plt.legend(loc='best')
ani = FuncAnimation(plt.gcf(), animate, interval=1000)


plt.tight_layout()
plt.show()
