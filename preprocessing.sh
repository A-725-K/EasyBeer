#!/bin/bash

set -o errexit
set -o nounset

DATASET_NAME='dataset.csv'
TEMP_FILE='temp.csv'
REGEX='^[a-zA-Z][a-zA-Z 0-9_-]*$'

function select_features() {
    if [ $# -ne 1 ]; then
	echo "This function require only 1 parameter !"
	exit
    fi

    echo $1 | cut -d "," -f 1,2,4,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
}

function preprocess_names() {
    if [ $# -ne 1 ]; then
	echo "This function require only 1 parameter !"
	exit
    fi
    
    first=0
    while read -r line; do
	# select only some features
	line=$(select_features "$line")
    
	if [ $first -eq 0 ]; then
	    echo $line > $TEMP_FILE
	    (( ++first ))
	    continue
	fi
    
	beerName=$(echo $line | cut -d "," -f 2)
    
	if [[ $beerName =~ $REGEX ]] && [[ ${#beerName} -ge 3 ]]; then
	    echo $line >> $TEMP_FILE
	fi

	echo $first
	(( ++first ))
    done < $1
}

rm -rf $TEMP_FILE
rm -rf $DATASET_NAME

preprocess_names $1

mv $TEMP_FILE $DATASET_NAME
