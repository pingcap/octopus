#!/bin/bash
set -e

run() {
CURDIR=$(cd `dirname $0`; pwd)
OUTDIR=$CURDIR/$1

if [ -e $OUTDIR ]; then
  rm -rf $OUTDIR
fi 

if [ ! -d $OUTDIR ]; then
  mkdir -p $OUTDIR
fi

cd $CURDIR
for file in `ls mysql/*.sql`; do
  name=${file%.*}
  name=${name##mysql\/}
  outputfile=$OUTDIR/$name.out
#  cnt=`jobs -p | wc -l`
#  if [ $cnt -ge "1" ]; then
#	for job in `jobs -p`;
#    	do
#        	wait $job
#		break
#    	done
#  fi
  echo "run $file > $outputfile"
  #mysql -h $2 -P $3 -u root -D tpch < $file >$outputfile &
  mysql -h $2 -P $3 -u root -D tpch < $file >$outputfile 
  if [ $? -ne 0 ]; then
    echo "mysql failed."
    exit 1
  fi
done

#for job in `jobs -p`;
#do
#	wait $job
#done
}

if [ $# -eq 0 ]; then
	run tidb_r "127.0.0.1" "4000"
else
	run $*
fi
