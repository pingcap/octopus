#!/bin/bash

PROCFILE="Procfile"

function wait_time {
    expr $RANDOM % 10 + 1
}

function cycle {
    for a; do
        echo "cycling $a"
        goreman -f $PROCFILE run stop $a || echo "could not stop $a"
        sleep `wait_time`s
        goreman -f $PROCFILE run restart $a || echo "could not restart $a"
    done
}

function cycle_members {
    cycle tikv1 tikv2 tikv3
}
function cycle_bridge {
    cycle bridge1 bridge2 bridge3
}

function kill_maj {
    idx="tikv"`expr $RANDOM % 3 + 1`
    idx2="$idx"
    while [ "$idx" == "$idx2" ]; do
        idx2="tikv"`expr $RANDOM % 3 + 1`
    done
    echo "kill majority $idx $idx2"
    goreman -f $PROCFILE run stop $idx || echo "could not stop $idx"
    goreman -f $PROCFILE run stop $idx2 || echo "could not stop $idx2"
    sleep `wait_time`s
    goreman -f $PROCFILE run restart $idx || echo "could not restart $idx"
    goreman -f $PROCFILE run restart $idx2 || echo "could not restart $idx2"
}

function kill_all {
    for a in tikv1 tikv2 tikv3; do
        goreman -f $PROCFILE run stop $a || echo "could not stop $a"
    done
    sleep `wait_time`s
    for a in tikv1 tikv2 tikv3; do
        goreman -f $PROCFILE run restart $a || echo "could not restart $a"
    done
}

function choose {
    fault=${FAULTS[`expr $RANDOM % ${#FAULTS[@]}`]}
    echo $fault
    $fault || echo "failed: $fault"
}

sleep 2s

FAULTS=(cycle_members kill_maj kill_all cycle_bridge)

while [ 1 ]; do
    choose
    for a in pd tikv1 tikv2 tikv3 tidb; do goreman -f $PROCFILE run start $a; done
    sleep 30
done