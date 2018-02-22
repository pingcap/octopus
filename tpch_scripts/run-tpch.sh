#!/bin/bash
set -e

run() {
CURDIR=$(cd `dirname $0`; pwd)
OUTDIR=$CURDIR/$1
TIMEDIR=$CURDIR/"cost_r"

if [ -e $OUTDIR ]; then
  rm -rf $OUTDIR
fi

if [ ! -d $OUTDIR ]; then
  mkdir -p $OUTDIR
fi

if [ -e $TIMEDIR ]; then
  rm -rf $TIMEDIR
fi

if [ ! -d $TIMEDIR ]; then
  mkdir -p $TIMEDIR
fi

cd $CURDIR
for file in `ls mysql/*.sql`; do
  name=${file%.*}
  name=${name##mysql\/}
  outputfile=$OUTDIR/$name.out
  outputtime=$TIMEDIR/$name.time
#  cnt=`jobs -p | wc -l`
#  if [ $cnt -ge "1" ]; then
#      	for job in `jobs -p`;
#      	do
#              	wait $job
#      		break
#      	done
#  fi
  echo "run $file > $outputfile"
  start=`date +%s%N`
  #mysql -h $2 -P $3 -u root -D tpch < $file >$outputfile &
  mysql -h $2 -P $3 -u root -D tpch < $file >$outputfile
  end=`date +%s%N`
  if [ $? -ne 0 ]; then
    echo "mysql failed."
    exit 1
  fi
  difTime=$[ end - start ]
  echo $difTime >$outputtime
done

#for job in `jobs -p`;
#do
#      wait $job
#done
}

if [ $# -eq 0 ]; then
    run tidb_r "127.0.0.1" "4000"
else
    run $*
fi