#!/bin/sh

# Script for unzipping and putting VCF files on HDFS
# TODO: add decryption as well

HDFS_DIRECTORY="/data/UK10K/"

for gz in *.gz
do
  echo $gz
  result=$(gunzip $gz)
  unzipped=`basename $gz .gz`
  eval "hdfs dfs -put $unzipped $HDFS_DIRECTORY"
  rm $unzipped
done