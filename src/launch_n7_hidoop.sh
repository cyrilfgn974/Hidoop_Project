javac ordo/*.java formats/*.java map/*.java

mate-terminal --window -e "/bin/bash -c \"java ordo/WorkerImpl 0; exec /bi\
n/bash\"" \
                --tab -e "/bin/bash -c \"java ordo/WorkerImpl 1; exec /bin\
/bash\"" \
                --tab -e "/bin/bash -c \"java ordo/WorkerImpl 2; exec /bin\
/bash\"" \
                --tab -e "/bin/bash -c \"java ordo/WorkerImpl 3; exec /bin\
/bash\""


printf '\n java application/MyMapReduce filesample.txt \n'

