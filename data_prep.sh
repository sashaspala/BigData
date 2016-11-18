#!/bin/bash

# inputloc = $1  <-This was causing problems, so I commented it out for now.
# I also got rid of the folder system since the output goes to hdfs.

mahout/bin/mahout seqdirectory -i professions -o seqdir -Dmapred.job.queue.name=hadoop02 #make sure lemma index is the only thing in this directory
mahout/bin/mahout seq2sparse -i seqdir -o sparse -lnorm -wt tfidf -Dmapred.job.queue.name=hadoop02

# Split processed data into training and test sets
mahout/bin/mahout split -i sparse --trainingOutput train --testOutput test --randomSelectionPct 30 --overwrite --sequenceFiles -xm sequential

#use respective training/testing locs for meghan's classifier bit
##training outputs a model based on the training data
##testing uses that model to test on the test data

mahout/bin/mahout trainnb -i train -o model -li labelindex -ow -Dmapred.job.queue.name=hadoop02
#where {Path_to_model} is where we want to store our model for later use