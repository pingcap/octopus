set -e

run() {
CURDIR=$(cd `dirname $0`; pwd)
TIMEDIR=$CURDIR/"cost_r"
echo $TIMEDIR

if [ -e $TIMEDIR ]; then
  rm -rf $TIMEDIR
fi

if [ ! -d $TIMEDIR ]; then
  mkdir -p $TIMEDIR
fi

cd $CURDIR
outputtime=$TIMEDIR/q1.time
start=`date +%s%N`

end=`date +%s%N`
difTime=$[ end - start ]
echo $difTime >$outputtime
}

run $*