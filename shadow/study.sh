#!/bin/bash

BASEDIR=`pwd`
STUDYDIR="studies/`date -Iseconds`"
echo $STUDYDIR
mkdir -p "$STUDYDIR"
git diff --submodule=diff > $STUDYDIR/git.diff
git status > $STUDYDIR/git.status
git describe --always > $STUDYDIR/git.describe

cd $STUDYDIR

echo "run,blobcount,batchPublish,bw,peercount,latency,received" > study.csv

for run in $(seq 5); do
  for blobcount in 3 4 6 8 12 16 32 48 64; do
  #for blobcount in 8; do
    sed -E -i "s/blobcount = [0-9]+/blobcount = $blobcount/" $BASEDIR/main.nim
    for batchPublish in "true" "false"; do
      sed -E -i "s/batchPublish = (true|false)/batchPublish = $batchPublish/" $BASEDIR/main.nim
      for bw in 10 20 30 50 70 100 150 200; do
  #    for bw in 20 100; do
        sed -E -i "s/: \"[0-9]+ Mbit\"/: \"$bw Mbit\"/" $BASEDIR/shadow.yaml.template
  #      for peercount in 100 500 1000 2000 3000 4000 5000; do
        for peercount in 1000; do
          lat_recv=$($BASEDIR/run.sh 1 1 $((peercount-1)) 0.0 "$BASEDIR" | tail -1)
          echo "$run,$blobcount,$batchPublish,$bw,$peercount,$lat_recv" >> study.csv
        done
      done
    done
  done
done
