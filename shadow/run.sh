#!/bin/sh

if [ $# -lt 3 ]; then
    echo "Usage: $0 <runs> <FastNodes> <SlowNodes> [PacketLoss(float0..1)]"
    exit 1
fi

BASEDIR=`pwd`
WORKDIR="results/`date -Iseconds`"
echo $WORKDIR
mkdir -p "$WORKDIR"
git diff --submodule=diff > $WORKDIR/git.diff
git status > $WORKDIR/git.status
git describe --always > $WORKDIR/git.describe

runs="$1"			#number of simulation runs
nodes1="$2"			#number of nodes in class 1
nodes2="$3"			#number of nodes in class 2
packet_loss=${4:-0.0}
nodes=$(($nodes1 + $nodes2))
shadow_file_base="shadow.yaml.template"
shadow_file="$WORKDIR/shadow.yaml"	
sed '/*FastHost/q' "$shadow_file_base" >"$shadow_file"
sed -E -i "s/\"PEERS\": \"[0-9]+\"/\"PEERS\": \"$nodes\"/" "$shadow_file"
sed -E -i "s/packet_loss [0-9\.]+/packet_loss $packet_loss/" "$shadow_file"

counter=2
while [ $counter -le $nodes1 ]; do
  echo "  peer$counter:
      <<: *FastHost" >> "$shadow_file"
  counter=$((counter + 1))
done
while [ $counter -le $nodes ]; do
  echo "  peer$counter:
      <<: *SlowHost" >> "$shadow_file"
  counter=$((counter + 1))
done


rm -f shadowlog* latencies* stats* main && rm -rf shadow.data/
nim c -d:chronicles_colors=None -d:chronicles_log_level=INFO -d:chronicles_sinks="textlines[stdout,nocolors]" --threads:on -d:metrics -d:libp2p_network_protocols_metrics -d:release main 

cp main "$WORKDIR" 
cd "$WORKDIR"

for i in $(seq $runs); do
    echo "Running for turn "$i
    shadow shadow.yaml > shadowlog$i && 
        grep -rne 'milliseconds\|BW' shadow.data/ > latencies$i && 
        grep -rne 'statcounters:' shadow.data/ > stats$i
    #rm -rf shadow.data/
done

for i in $(seq $runs); do
    echo "Summary for turn "$i | tee -a summmary.txt
    grep -R "Mesh size C0" | awk '{ total += $4; count++ } END { print "Mesh size C0 = " total/count }' | tee -a summmary.txt
    awk -f $BASEDIR/summary_latency.awk latencies$i | tee -a summmary.txt
    awk -f $BASEDIR/summary_shadowlog.awk shadowlog$i | tee -a summmary.txt
    awk -f $BASEDIR/summary_dontwant.awk stats$i | tee -a summmary.txt   
done

grep  "\"RX\"" shadow.data/hosts/peer*/main.1000.stdout >rx.csv
python "$BASEDIR/plot_rx.py"
