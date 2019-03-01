#!/bin/bash

set -o errexit
set -o nounset

DATASET_NAME='dataset.csv'
TEMP_FILE='temp.csv'
REGEX='^[a-zA-Z][a-zA-Z 0-9_-]*$'
IDX=1

function select_features() {
    if [ $# -ne 1 ]; then
	echo "select_features: This function require only 1 parameter !"
	exit
    fi

    echo $1 | cut -d "," -f 1,2,4,6,7,8,9,10,11,12,13,14,15,17,18,20
}


function preprocess_names() {
    if [ $# -ne 1 ]; then
	echo "preprocess_names: This function require only 1 parameter !"
	exit
    fi
    
    first=0
    while read -r line; do
	# select only some features
	line=$(select_features "$line")
    
	if [ $first -eq 0 ]; then
	    echo $line > "$IDX$TEMP_FILE"
	    (( ++first ))
	    continue
	fi
    
	beerName=$(echo $line | cut -d "," -f 2)
    
	if [[ $beerName =~ $REGEX ]] && [[ ${#beerName} -ge 3 ]]; then
	    echo $line >> "$IDX$TEMP_FILE"
	fi

	echo $first
	(( ++first ))
    done < $1
}

function specgrav_2_plato() {
    if [ $# -ne 1 ]; then
	echo "specgrav_2_plato: This function require only 1 parameter !"
	exit
    fi

    sg=$1
    sg2=$(bc -l <<< $sg^2 )
    sg3=$(bc -l <<< $sg^3 )
    
    echo $(bc -l <<< "scale=1; (135.997*$sg3 - 630.272*$sg2 + 1111.14*$sg - 616.868)/1")
}

function preprocess_degree() {
    if [ $# -ne 1 ]; then
	echo "preprocess_degree: This function require only 1 parameter !"
	exit
    fi

    #while read -r line; do
	
    #done < $1	
}

rm -rf *$TEMP_FILE
rm -rf $DATASET_NAME

preprocess_names $1
#(( ++IDX ))
#preprocess_degree "$IDX$TEMP_FILE"
mv "$IDX$TEMP_FILE" $DATASET_NAME
