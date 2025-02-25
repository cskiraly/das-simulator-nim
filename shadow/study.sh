#!/bin/bash

BASEDIR=`pwd`
STUDYDIR="studies/`date -Iseconds`"
echo $STUDYDIR
mkdir -p "$STUDYDIR"
git diff --submodule=diff > $STUDYDIR/git.diff
git status > $STUDYDIR/git.status
git describe --always > $STUDYDIR/git.describe

cd $STUDYDIR

echo "run,blocksize,degree,strategy,threshold,delay,bw,peercount,latency,received,dup,dupv" > study.csv

for run in $(seq 5); do
  for blocksize in "102400"; do
    sed -E -i "s/blocksize = [0-9\^]+/blocksize = $blocksize/" $BASEDIR/main.nim
  #for blobcount in 3 4 6 8 12 16 32 48 64; do
  #for blobcount in 8; do
    # sed -E -i "s/blobcount = [0-9]+/blobcount = $blobcount/" $BASEDIR/main.nim
    # for batchPublish in "true" "false"; do
    #   sed -E -i "s/batchPublish = (true|false)/batchPublish = $batchPublish/" $BASEDIR/main.nim
      for bw in 20; do
  #    for bw in 20 100; do
        sed -E -i "s/: \"[0-9]+ Mbit\"/: \"$bw Mbit\"/" $BASEDIR/shadow.yaml.template
  #      for peercount in 100 500 1000 2000 3000 4000 5000; do
        for peercount in 1000; do
          #for degree in 8 6 4; do
          for degree in 8; do
            sed -E -i "s/gossipSub.parameters.d = [0-9]+/gossipSub.parameters.d = $degree/" $BASEDIR/udpnetwork.nim
            for strategy in suppressAbove suppressIfSeen suppressOnHops; do
              sed -E -i "s/suppressStrategy = [[:alnum:]]+/suppressStrategy = $strategy/" $BASEDIR/vendor/nim-libp2p/libp2p/protocols/pubsub/gossipsub.nim
              for threshold in 0 1 3 5 7 9; do
                sed -E -i "s/suppressThreshold = [0-9]+/suppressThreshold = $threshold/" $BASEDIR/vendor/nim-libp2p/libp2p/protocols/pubsub/gossipsub.nim
                for delay in 0 30; do
                  sed -E -i "s/validationDelay = [0-9]+/validationDelay = $delay/" $BASEDIR/main.nim
                  RUNID="r$run-b$blocksize-d$degree-s$strategy-t$threshold-d$delay-bw$bw-n$peercount"
                  lat_recv=$($BASEDIR/run.sh 1 1 $((peercount-1)) 0.0 "$BASEDIR" "$RUNID" | tail -1)
                  echo "$run,$blocksize,$degree,$strategy,$threshold,$delay,$bw,$peercount,$lat_recv" >> study.csv
                done
              done
            done
          done
        done
      done
    # done
  done
done
