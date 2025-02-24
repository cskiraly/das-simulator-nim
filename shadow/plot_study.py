import pandas as pd
import seaborn as sns
import numpy as np

# sns.set_theme()
# sns.set_context("talk")

# blobcount,batchPublish,bw,peercount,latency
study = pd.read_csv("study.csv")

ax = sns.relplot(study, kind="line", x='bw', y='latency', hue='batchPublish', col='blobcount')
ax.set(xlabel='node bandwidth (uplink and downlink) [Mbps]', ylabel='average time to custody [ms]')
fig = ax._figure
fig.savefig("study_by_bw.png")
fig.clf()

ax = sns.relplot(study[study['received'] >= study['peercount'] - 10 ],
                 kind="line", x='blobcount', y='latency',
                 #hue=study[['bw', 'batchPublish']].apply(tuple, axis=1)
                 hue='bw', col='batchPublish',
                 legend='full',
                 palette='tab10'
                 )
ax.legend.set_title("bw [Mbps]")
ax.set(xlabel='blob count', ylabel='average time to custody [ms]')
fig = ax._figure
fig.savefig("study_by_blobcount.png")
fig.clf()
