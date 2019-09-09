#!/bin/bash

DATA='beers.csv'
STYLES='style.csv'
TMP='tmp.csv'
OUTPUT='ml_data.csv'

rm -rf $OUTPUT
rm -rf $TMP

declare -A styles

while IFS=, read -r id s; do
	styles+=([$s]=$id)
done<$STYLES

i=0
while read -r line; do
	newL=$(echo $line | cut -d ',' -f 4,5,6,7,8,9,10,11,12,13)
	styleN=${styles[$(echo $line | cut -d ',' -f 3)]}
	echo "$newL,$styleN" >> $TMP
	echo "...$i..."
	(( ++i ))
done<$DATA

cat $TMP | grep -v 'N/A' | sort --numeric-sort --field-separator=',' --key=11 > $OUTPUT
rm -rf $TMP
