#!/bin/bash
pcount=$#
if((pcount==0));then
echo no args
exit;
fi

p1=$1
fname=`basename $p1`
echo fname=$fname

pdir=`cd -P $(dirname $p1); pwd`
echo pdir=$pdir

user=`whoami`

for((host=1;host<4;host++));do
  echo --------------spark$host--------------
  rsync -rvl $pdir/$fname $user@spark$host:$pdir
done
