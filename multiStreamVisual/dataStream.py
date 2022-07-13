import matplotlib.pyplot as plt
import numpy as np
from matplotlib.animation import FuncAnimation
import psutil
import collections


# function to update the data
def my_function(i):
    # get data
    cpu.popleft()
    cpu.append(psutil.cpu_percent())
    # clear axis
    cpuPlot.cla()
    # plot cpu
    cpuPlot.plot(cpu)
    cpuPlot.scatter(len(cpu)-1, cpu[-1])
    cpuPlot.text(len(cpu)-1, cpu[-1]+2, "{}%".format(cpu[-1]))
    cpuPlot.set_ylim(0,100)
    #print('hi')

# start collections with zeros
cpu = collections.deque(np.zeros(10))
# define and adjust figure
fig = plt.figure(figsize=(12,6), facecolor='#DEDEDE')
cpuPlot = plt.subplot(121)
cpuPlot.set_facecolor('#DEDEDE')
# animate
ani = FuncAnimation(fig, my_function, interval=1000)
print("hi")
plt.show()

