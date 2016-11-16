#!/bin/bash

#Usage: ./run.sh <location of training input lemma indices> <location of testing input>
training_index_loc = $1
testing_index_loc = $2

#run first map reduce job to classify training data
yarn jar uber-BigData_A2-0.0.1-SNAPSHOT.jar professions.ProfessionIndexMapred -Dmapred.job.queue.name=hadoop02 $index_loc output/data

#call data_prep on first mr job
./data_prep.sh output/data

#now run naive bayes
yarn jar uber-BigData_A2-0.0.1-SNAPSHOT.jar professions.NaiveBayes -Dmapred.job.queue.name=hadoop02 model/model $testing_index_loc nboutput/data model/labelindex

#finally, run evaluation map reduce job
yarn jar uber-BigData_A2-0.0.1-SNAPSHOT.jar professions.ProfessionIndexMapred -Dmapred.job.queue.name=hadoop02 nboutput evaluate/mr

#and return evaluation post-processing
java ProcessEvaluation evaluate/mr
