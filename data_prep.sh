#!/bin/bash


./mahout seqdirectory -i <path to shortened lemma index folder> -o <path to desired output location>  #make sure lemma index is the only thing in this directory
./mahout seq2sparse -i <output location from above command> -o <path to desired output location> -lnorm -wt tfidf
./mahout split -i <output location from seq2sparse command> --trainingOutput <desired output location for training data> --testOutput <desired loc for test data> --randomSelectionPct 40 --overwrite --sequenceFiles -xm sequential

#use respective training/testing locs for meghan's classifier bit
##training outputs a model based on the training data
##testing uses that model to test on the test data

./mahout trainnb -i ${PATH_TO_TFIDF_VECTORS} -o ${PATH_TO_MODEL}/model -li ${PATH_TO_MODEL}/labelindex -ow
#where {Path_to_model} is where we want to store our model for later use
