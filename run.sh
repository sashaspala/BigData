#!/bin/bash

#Usage: ./run.sh <location of training input lemma indices> <location of testing input>
# training_index_loc = $1 <- This was causing problems so I commented it out for now.
# testing_index_loc = $2

#run first map reduce job to classify training data
yarn jar uber-BigData_A2-0.0.1-SNAPSHOT.jar professions.ProfessionIndexMapred input professions

#call data_prep on first mr job
./data_prep.sh

#now run naive bayes
yarn jar uber-BigData_A2-0.0.1-SNAPSHOT.jar professions.NaiveBayes model test nboutput labelindex

#finally, run evaluation map reduce job
yarn jar uber-BigData_A2-0.0.1-SNAPSHOT.jar professions.ProfessionIndexMapred nboutput mr

#and return evaluation post-processing
java ProcessEvaluation mr
