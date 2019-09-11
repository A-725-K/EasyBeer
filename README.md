# EasyBeer
University project for Advanced Data Management (ADM) course a.y. 2018/2019.

## About the project
We would like to explore technical data about beers in order to realize the backend for a future application based on NOSql technologies for data analysis. Our work is divided in two parts:
  * **Filters**: to wisely choose a subset of beers wrt some criterion</li>
  * **Analysis**: to build models, to clusterize the dataset or to carry our more advanced queries</li>
The former request is implemented in *Cassandra* while for the latter we chose *Spark*. While our queries have static parameters, the idea is to let the users of the application chose according to their tastes, where it is possible.

## Getting started
First of all you have to clone or download this repository, with the command:
```bash
git clone https://github.com/A-725-K/EasyBeer.git
```
Then you must have installed on your system both *Apache Cassandra*, and its interactive shell **cqlsh** too, and *Apache Spark* .

## How to use it
**1. Cassandra**<br>
First of all you have to change your current directory with the one of the aggregate you would like to use with the command:
```bash
cd Cassandra/beers_by_<aggregate-name>
```
Then you have to connect to *Cassandra* using its interactive shell using the command:
```SQL
cqlsh <host-addr>
```
After that you have to create a key space or to use an existing one. In our experiment we have adopted one called *ks_user10*, so, in order to launch without errors our scripts, you should adopt the same name for your key space or to change each *USE* directive at the beginning of our scripts. Then you have to load the table, load the data and try our workload with the following commands:
```SQL
cqlsh> SCRIPT 'create_table.cql';
cqlsh> SCRIPT 'insert_data.cql';
cqlsh> SCRIPT 'workload.cql';
```
If you wold try different aggregates you can build on your own simply going into directory *dataset*, launching the bash script *build_cql.sh* and following the instructions given to you by the program itself.
<br><br>

**2. Spark**<br>
First of all you have to put the datasets on *HDFS* in a dircetory called *input*, the dataset used in a query is specified inside the reference driver program. You can do it in this way:
```bash
hdfs dfs -mkdir input
hdfs dfs -put <local-dataset-name.csv> input/
```
Then, to execute our workload with the *Spark* framework it is sufficient to go into the directory *Spark/spark* with the command:
```bash
cd Spark/spark
```
and then launch the script *compile.sh*
```bash
./compile.sh ARG
```
where *ARG* could be one of the following:
  * <*filename*> (without the *.java* extension): to execute the correspondent query
  * C: to clean the working directory, it removes all *.class* files
  * CJAR: to remove all *.jar* files builded from previous executions
Finally you have only to wait for your results.

## Authors
* **<i>Andrea Canepa</i>** - Computer Science, UNIGE - *S 4185248*
* **<i>Riccardo Bianchini</i>** - Computer Science, UNIGE - *S 4231932*

