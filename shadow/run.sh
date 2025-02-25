#!/bin/sh

if [ $# -lt 3 ]; then
    echo "Usage: $0 <runs> <FastNodes> <SlowNodes> [PacketLoss(float0..1)] [BASEDIR]"
    exit 1
fi

BASEDIR=${5:-`pwd`}
WORKDIR="results/"${6:-`date -Iseconds`}
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
shadow_file_base="$BASEDIR/shadow.yaml.template"
shadow_file="$WORKDIR/shadow.yaml"	
sed '/*FastHost/q' "$shadow_file_base" >"$shadow_file"
sed -E -i "s/\"PEERS\": \"[0-9]+\"/\"PEERS\": \"$nodes\"/" "$shadow_file"
sed -E -i "s/packet_loss [0-9\.]+/packet_loss $packet_loss/" "$shadow_file"

counter=2
while [ $counter -le $nodes1 ]; do
  echo "  peer$counter:
      <<: *FastHost
      network_node_id: $counter" >> "$shadow_file"
  counter=$((counter + 1))
done
while [ $counter -le $nodes ]; do
  echo "  peer$counter:
      <<: *SlowHost
      network_node_id: $counter" >> "$shadow_file"
  counter=$((counter + 1))
done


rm -f shadowlog* latencies* stats* main && rm -rf shadow.data/
cd $BASEDIR
nim c -d:chronicles_colors=None -d:chronicles_log_level=INFO -d:chronicles_sinks="textlines[stdout,nocolors]" --threads:on -d:metrics -d:libp2p_network_protocols_metrics -d:release main 
cd -

cp $BASEDIR/main "$WORKDIR" 
ln -s $BASEDIR/networkmodel/atlas_v201801.shadow_v2.gml "$WORKDIR"
cd "$WORKDIR"

for i in $(seq $runs); do
    echo "Running for turn "$i
    shadow shadow.yaml > shadowlog$i && 
        #grep -rne 'milliseconds\|BW' shadow.data/ > latencies$i && 
        grep -rne 'milliseconds' shadow.data/ > latencies$i && 
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
python "$BASEDIR/plot_arr.py"

#rm -f shadowlog* && rm -rf shadow.data/

stats=$(awk '
   /^0 / { lat_total += $2;rcv_total +=$3; count++ }
   /^dup_received/ { dup_total += $9; dup_count++ }
   /^dupv_received/ { dupv_total += $9; dupv_count++ }
   END { print lat_total/count","rcv_total/count","dup_total/count","dupv_total/count }' summmary.txt )
echo $stats
