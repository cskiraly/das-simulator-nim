import pandas as pd
import seaborn as sns
import numpy as np

# sns.set_theme()
# sns.set_context("talk")

# read RX file with a line for each RX event, with:
# - first received hopcount
# - actual hopcount
# - relative delay from first receive 
rx = pd.read_csv("rx.csv", sep='[\s,\(\)]+', engine='python', names=['file', 'dir', 'msgid', 'firsthops', 'hopcount', 'delayms', 'a'])

# distribution of relative RX delay, per firsthop count
fig = sns.ecdfplot(rx, x='delayms', hue='firsthops').get_figure()
fig.savefig("rx.png")
fig.clf()

# rx count, as a function of firsthop count
rxstats = rx.groupby(["file"]).agg(firsthops=('firsthops', 'mean'), rxcount=('firsthops', len))
#print(rxstats)
fig = sns.lineplot(rxstats, x='firsthops', y='rxcount',
                   errorbar=('pi',100)).get_figure()
fig.savefig("rx_per_firsthops_minmax.png")
fig.clf()
fig = sns.lineplot(rxstats, x='firsthops', y='rxcount',
                   errorbar='sd').get_figure()
fig.savefig("rx_per_firsthops_sd.png")
fig.clf()

# first RX delay distribution
firstrx = pd.read_csv("latencies1", sep='[\s:]+', engine='python', names=['file', 'line', 'msgid', 'dummy', 'rxlatency'])
firstrx[['rxlatency']] = firstrx[['rxlatency']].astype(int)
fig = sns.ecdfplot(firstrx[firstrx['msgid'] == '0'], x='rxlatency').get_figure()
fig.savefig("firstrx.png")
fig.clf()

