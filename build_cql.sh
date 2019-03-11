#!/bin/bash

# for debug
set -o errexit
set -o nounset

# datasets
PUBS='pubs.csv'
BEERS='dataset.csv'
SUPPL='suppliers.csv'

# output file
OUT="create_insert.cql"

# types constant
TEXT='text'
INT='int'
FLOAT='float'

# hasmaps
declare -A indexes
declare -A types

# initialize types' hashmap
function initialize_types() {
    if [ $# -ne 0 ]; then
	echo 'initialize_types: This function requires no parameter !'
	exit -1
    fi
    
    # pubs
    types["id_pub"]=$INT
    types["name_pub"]=$TEXT
    types["phone_pub"]=$TEXT
    types["address_pub"]=$TEXT
    types["covers"]=$INT

    # suppliers
    types["id_supp"]=$INT
    types["name_supp"]=$TEXT
    types["phone_supp"]=$TEXT
    types["head_office"]=$TEXT
    types["email"]=$TEXT

    # beers
    types["BeerID"]=$INT
    types["Name"]=$TEXT
    types["Style"]=$TEXT
    types["Size(L)"]=$FLOAT
    types["OG"]=$FLOAT
    types["FG"]=$FLOAT
    types["ABV"]=$FLOAT
    types["IBU"]=$FLOAT
    types["Color"]=$FLOAT
    types["BoilSize"]=$FLOAT
    types["BoilTime"]=$FLOAT
    types["BoilGravity"]=$FLOAT
    types["Efficiency"]=$FLOAT
    types["BrewMethod"]=$TEXT
    types["PrimaryTemp"]=$FLOAT
}

# show the columns of the datasets indexed
function show_cols() {
    if [ $# -ne 0 ]; then
	echo 'show_cols: This function requires no parameter !'
	exit -1
    fi

    idx=1

    echo 'Features of datasets:'
    echo '----------------------------------'
    
    headP=$(head -1 $PUBS)
    for field in ${headP//,/ }; do
	echo "$idx)  $field"
	indexes["$idx"]=$field
	(( ++idx ))
    done

    echo '----------------------------------'
    
    headS=$(head -1 $SUPPL)
    for field in ${headS//,/ }; do
	echo "$idx)  $field"
	indexes["$idx"]=$field
	(( ++idx ))
    done

    echo '----------------------------------'
    
    headB=$(head -1 $BEERS)
    for field in ${headB//,/ }; do
	echo "$idx)  $field"
	indexes["$idx"]=$field
	(( ++idx ))
    done

    echo '----------------------------------'
}

function insert_nl() {
    echo -e "$1\n"
}

# compose the create table statement
function create_table() {
    if [ $# -ne 4 ]; then
    	echo 'create_table: This function requires 4 parameters !'
    	exit -1
    fi

    idxs=$2
    str="CREATE TABLE $1 (\n\t"
    echo "DROP TABLE IF EXISTS $1;" >> $OUT

    for i in $2; do
	col=${indexes[$i]}
	type="${types[$col]}"
	str="$str$col $type,\n\t"
    done
    str="${str}PRIMARY KEY()\n);"
    echo -e $str
}

############
### MAIN ###
############

rm -rf $OUT

initialize_types
show_cols

echo -n 'Insert table name:   '
read -e tableName

echo -n "Insert index of columns separated with ' ':   "
read -e idxs
nCols=$(echo $idxs | awk -F' ' '{print NF}')

keyN=0
partN=1
while [[ $partN -gt $keyN ]]; do
    echo -n 'How many columns are primary key ?   '
    read keyN
	  
    echo -n 'How many columns are partition key?   '
    read partN

    if [[ $partN -gt $keyN ]]; then
	echo 'ERROR: There must be less columns in the partition key than columns in the primary key !'
    elif [[ $keyN -gt $nCols ]]; then
	echo 'ERROR: There must be less columns in the primary key than columns in the whole table !'
	keyN=0
	partN=1
    elif [[ $partN -gt $nCols ]]; then
	echo 'ERROR: There must be less columns in the partition key than columns in the whole table !'
	keyN=0
	partN=1
    fi
done

create_table $tableName "$idxs" #$keyN $partN

