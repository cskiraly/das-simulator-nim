import pandas as pd
import seaborn as sns
import numpy as np

# sns.set_theme()
# sns.set_context("talk")

rx = pd.read_csv("rx.csv", sep='[\s,\(\)]+', names=['file', 'dir', 'msgid', 'firsthops', 'hopcount', 'delayms', 'a'])

fig = sns.ecdfplot(rx, x='delayms', hue='firsthops').get_figure()
fig.savefig("rx.png")
fig.clf()

rxstats = rx.groupby(["file"]).agg(firsthops=('firsthops', np.mean), rxcount=('firsthops', len))
print(rxstats)
fig = sns.lineplot(rxstats, x='firsthops', y='rxcount').get_figure()
fig.savefig("rx2.png")
fig.clf()

firstrx = pd.read_csv("latencies1", sep='[\s:]+', names=['file', 'line', 'msgid', 'dummy', 'rxlatency'])
fig = sns.ecdfplot(firstrx, x='rxlatency').get_figure()
fig.savefig("firstrx.png")
fig.clf()

