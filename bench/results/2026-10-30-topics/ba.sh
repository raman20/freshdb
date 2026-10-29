#!/bin/bash
# interleaved before/after topic benches; run on a quiet box
cd /workspace/topic
: > ba_lens_before.txt; : > ba_lens_after.txt; : > ba_root_before.txt; : > ba_root_after.txt
for i in 1 2 3; do
 for w in before after; do
  ./lens_$w.test -test.run XXX -test.bench 'TopicApply|TopicSnapshotLoad' -test.benchtime 3x -test.count 1 >> ba_lens_$w.txt 2>&1
  ./lens_$w.test -test.run XXX -test.bench 'TopicRead100|TopicDropSubject' -test.benchtime 1s -test.count 1 >> ba_lens_$w.txt 2>&1
  ./root_$w.test -test.run XXX -test.bench 'Topic' -test.benchtime 2s -test.count 1 >> ba_root_$w.txt 2>&1
 done
done
echo BADONE
