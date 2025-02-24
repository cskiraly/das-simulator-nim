import pandas as pd
import seaborn as sns
import numpy as np
import glob
import re

# sns.set_theme()
# sns.set_context("talk")
sns.set_theme(rc={'figure.figsize':(50,50)})

# log example: shadow.data/hosts/peer100/main.1000.stdout:946684985000000000 ARR ms: 75 r0c59 1/16
path = "shadow.data/hosts/peer*/main.1000.stdout"
filename_pattern = "shadow.data/hosts/peer(\d+)/main.1000.stdout"
log_pattern = r'(\d+) ARR ms: (\d+) r(\d+)c(\d+) (\d+)/(\d+)'
arrl = []
for filename in glob.glob(path):
    match = re.match(filename_pattern, filename)
    peer = match.group(1)
    with open(filename, 'r') as f:
        for line in f:
            match = re.match(log_pattern, line)
            if match:
                arrl.append([peer] + list(match.groups()))

arr = pd.DataFrame(arrl, columns=['peer', 'msgid', 'delayms', 'row', 'column', 'counter', 'interest'])
arr[['delayms', 'counter', 'interest']] = arr[['delayms', 'counter', 'interest']].astype(int)

#fig = sns.ecdfplot(arr, x='delayms', hue='column').get_figure()
fig = sns.displot(arr, x='delayms', kind='ecdf', hue='peer', legend=False)
#sns.move_legend(fig, "center right", ncols=7)
fig.savefig("arr_peer.png")

fig = sns.displot(arr, x='delayms', kind='ecdf', hue='column', legend=False)
fig.savefig("arr_col.png")

exit()

rxstats = rx.groupby(["file"]).agg(firsthops=('firsthops', np.mean), rxcount=('firsthops', len))
print(rxstats)
fig = sns.lineplot(rxstats, x='firsthops', y='rxcount').get_figure()
fig.savefig("rx2.png")
fig.clf()

firstrx = pd.read_csv("latencies1", sep='[\s:]+', names=['file', 'line', 'msgid', 'dummy', 'rxlatency'])
fig = sns.ecdfplot(firstrx, x='rxlatency').get_figure()
fig.savefig("firstrx.png")
fig.clf()

