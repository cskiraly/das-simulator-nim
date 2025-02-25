import pandas as pd
import seaborn as sns
import numpy as np

# sns.set_theme()
# sns.set_context("talk")

# run,blocksize,degree,strategy,threshold,delay,bw,peercount,latency,received,dup,dupv
study = pd.read_csv("study.csv")

print(study[study['degree'] == 8])
ax = sns.relplot(study[study['degree'] == 8], kind="scatter", y='dup', x='latency',
                 hue='strategy')#, col='delay', row='threshold')#, errorbar=None)
ax.set(ylabel='duplicates', xlabel='average latency [ms]')
fig = ax._figure
fig.savefig("study_strategies1.png")
fig.clf()

# ax = sns.relplot(study[study['received'] >= study['peercount'] - 10 ],
#                  kind="line", x='blobcount', y='latency',
#                  #hue=study[['bw', 'batchPublish']].apply(tuple, axis=1)
#                  hue='bw', col='batchPublish',
#                  legend='full',
#                  palette='tab10'
#                  )
# ax.legend.set_title("bw [Mbps]")
# ax.set(xlabel='blob count', ylabel='average time to custody [ms]')
# fig = ax._figure
# fig.savefig("study_by_blobcount.png")
# fig.clf()
