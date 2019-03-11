#!/bin/bash

# useful settings for debug
set -o errexit
set -o nounset

# constants
DATASET_NAME='dataset.csv'
TEMP_FILE='temp.csv'
REGEX='^[a-zA-Z][a-zA-Z 0-9_-]*$'

# select only a suset of features among all
function select_features() {
    if [ $# -ne 1 ]; then
	echo "select_features: This function require only 1 parameter !"
	exit
    fi

    echo $1 | cut -d "," -f 1,2,4,6,7,8,9,10,11,12,13,14,15,17,18,20
}

# delete those beers with strange name or style with strange characters or symbols
function preprocess_names_and_styles() {
    if [ $# -ne 1 ]; then
	echo "preprocess_names: This function require only 1 parameter !"
	exit
    fi
    
    first=0
    while read -r line; do
	# select only some features
	line=$(select_features "$line")
    
	if [ $first -eq 0 ]; then
	    echo $line > "1$TEMP_FILE"
	    (( ++first ))
	    continue
	fi
    
	beerName=$(echo $line | cut -d "," -f 2)
	beerStyle=$(echo $line | cut -d "," -f 3)
    
	if [[ $beerName =~ $REGEX ]] && [[ ${#beerName} -ge 3 ]] && [[ $beerStyle =~ $REGEX ]] && [[ ${#beerStyle} -ge 3 ]]; then
	    echo $line >> "1$TEMP_FILE"
	fi

	(( ++first ))
	if [ $(( first % 500)) -eq 0 ]; then
	    echo -n '.'
	fi
    done < $1
}

# converts Specific Gravity to Plato Degrees
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

# delete the column of unit measure of boil gravity and convert the boil gravity
# from "Specific gravity" to "Plato degrees"
function preprocess_degree() {
    if [ $# -ne 1 ]; then
	echo "preprocess_degree: This function require only 1 parameter !"
	exit
    fi

    first=0
    nFields=$(head -1 $1 | awk -F',' '{print NF}')
    while read -r line; do
	str=""
    	readarray -td',' fields <<< "$line,"
    	unset 'fields[-1]'
    	declare -p fields > /dev/null
	
    	if [ $first -eq 0 ]; then
    	    for (( i=0; i<nFields; i++ )); do
    		if [ $i -ne 13 ]; then
		    if [ $i -eq $(( nFields-1 )) ]; then
			str="$str${fields[$i]}"
		    else
    			str="$str${fields[$i]},"
		    fi
    		fi
    	    done
	    echo $str > "2$TEMP_FILE"
	    (( ++first ))
    	    continue
    	fi
	
    	for (( i=0; i<$nFields; i++ )); do
    	    if [ $i -ne 13 ]; then # 13 == SugarScale
		if [ $i -eq $(( nFields-1 )) ]; then
		    str="$str${fields[$i]}"
    		elif [ $i -eq 11 ]; then # 11 == BoilGravity 
    		    if [ "${fields[13]}" == "Plato" ]; then
    			str="$str${fields[$i]},"
    		    else
    			pl=$(specgrav_2_plato ${fields[$i]})
			str="$str$pl,"
    		    fi 
    		else
		    str="$str${fields[$i]},"
		fi
    	    fi
    	done
	echo $str >> "2$TEMP_FILE"

	(( ++first ))
	if [ $(( first % 500)) -eq 0 ]; then
	    echo -n '.'
	fi
    done < $1
}

##################
###### MAIN ######
##################

# checks on input
if [ $# -ne 1 ]; then
    echo "Usage: ./preprocessing.sh <dataset_name>"
    exit
fi
if [ ! -f $1 ]; then
    echo "Error: file '$1' does not exists"
    exit
fi

# cleaning files from previous executions
rm -rf *$TEMP_FILE
rm -rf $DATASET_NAME

# pre-processing dataset
echo -n 'Processing names and styles of beers'
preprocess_names_and_styles $1
echo ''
echo -n 'Processing sugar scale'
preprocess_degree "1$TEMP_FILE"
echo ''

# changing name to clean dataset
mv "2$TEMP_FILE" $DATASET_NAME

# removing all temporary files
rm -rf *$TEMP_FILE
