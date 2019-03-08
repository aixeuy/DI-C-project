D-I C Project: Implementation of Yarowsky's Algo.
================================================

## data collecttion:
Collect data for word disambiguation of 'bank': finace vs. river  

run:  
mkdir data  
python collect_data.py  
cat data/* > data/bank_final.txt  
rm data/*tmp*  

Each line is a sentense conatining 'bank'.  
financial bank: line ends with '+'.  
river bank: line ends with '-'.
unknown: line ends with '0'.
