#!/bin/bash

inputloc = $1

mkdir data
mkdir data/seqdir
mkdir data/sparse
mkdir model
mkdir model/model
mkdir model/labelindex

./mahout seqdirectory -i $inputloc -o data/seqdir #make sure lemma index is the only thing in this directory
./mahout seq2sparse -i data/seqdir -o data/sparse -lnorm -wt tfidf

#use respective training/testing locs for meghan's classifier bit
##training outputs a model based on the training data
##testing uses that model to test on the test data

./mahout trainnb -i data/sparse -o model/model -li model/labelindex -ow
#where {Path_to_model} is where we want to store our model for later use
