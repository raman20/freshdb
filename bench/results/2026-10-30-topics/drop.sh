#!/bin/bash
cd /workspace/topic
: > drop_before.txt; : > drop_after.txt
for i in 1 2 3; do for w in before after; do
 ./drop_$w.test -test.run XXX -test.bench 'TopicDropSubject' -test.benchtime 64x -test.count 1 >> drop_$w.txt 2>&1
done; done
cd /workspace/goli2 && go test -run XXX -bench 'TopicRetainOpen|TopicOpen200k' -benchtime 3x -count 3 . > /workspace/topic/retain.txt 2>&1
echo DRDONE
