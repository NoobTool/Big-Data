### AIM
To implement the use of **MapReduce** to perform **string manipulation** tasks on **big data** present on **AWS EMR cluster** using **hadoop**. The initial testing was done using docker and creating a local cluster
and the final output was showcased on the EMR cluster.


 
### INSTRUCTIONS
---

It is to be made sure that the input files (3littlepigs, Melbourne, RMIT) are to be placed in a separate folder for the command below to run properly. The basic syntax of the command to execute a jar file is "hadoop jar <jar-file> <class-path> <input-file> <output-folder>"


- After logging in to the EMR cluster and storing the jar file in the directory /user/XXXXX/ and the input files in say /user/XXXXX/<input-folder-name>/ where XXXX is the student or teacher number.
- The command "hdfs dfs -copyToLocal <local_jar> ~/", to be used to copy the jar file to the local dir.

#### Using MapReduce with different approaches.
---

1. After the above steps, we need to run the following command "hadoop jar <jar-file-name> <classpath> <input-files> <output-folder> ~/" which in this case would be,
   "hadoop jar WordLengthCategorizer-0.0.1-SNAPSHOT.jar edu.rmit.cosc2367.s3853868.WordLengthCategorizer.<name-of-class-to-run> /user/XXXX/<input-folder-name>/* /user/XXXX/<output-folder-name> ~/"
   
   where XXXX is the user name, input-folder-name needs to be replaced by the name of the input folder given, and output-folder-name is the name given to the output folder.
   
   An example command - "hadoop jar WordLengthCategorizer-0.0.1-SNAPSHOT.jar edu.rmit.cosc2367.s3853868.WordLengthCategorizer.WordCountPreserved /user/root/input/* /user/root/output ~/"
   
2. The output destination with the output files will be created.

#### Using MapReduce to compute frequency.
---

To calculate the frequency of pair of words, the sample command can be used :-

hadoop jar /tmp/WordPairCounter-0.0.1-SNAPSHOT.jar edu.rmit.cosc2367.s3853868.WordPairCounter.WordPairCount /user/root/input/sample /user/root/output ~/


#### Using MapReduce to compute relative frequency.
---

To calculate the relative frequency of pair of words (example below), the following sample command can be used :-

hadoop jar /tmp/WordPairCounter2-0.0.1-SNAPSHOT.jar edu.rmit.cosc2367.s3853868.WordPairCounter.WordPairFrequency /user/root/input/sample /user/root/output2 ~/


Consider an example text below.
+++++++++
Hello, to the big world of the data.
The quick brown fox jumped into the big data.
+++++++++


The co-occurence of the pair (the,big) would be = (3/17), as 'the' occurs twice with the world big in line 1, which makes it (2/8) and occurs once in line 2 which makes it (1/9).
Therefore,co-occurences = (number of occurences/ total words) = 3/17.



### ANALYSIS OF MapReduce
---

There are a number of comparisons made down below while executing in-mapper combiner with and without preserving state across documents and some differences can be easily observed.

Note - There are 2 screenshots of the job counters and other parameters;calculated during the execution of both types of map-reduce is present by the name "Screenshot1" and "Screenshot2" in this zip which can be referenced. The screenshots are divided in 2 parts horizontally where the left one describes the parameters of "Not Preserved" and right one tells about the "Preserved" one.


1. Time Spent in Map Tasks

The total time spent in all map task while preserving state is 18318 ms while it is 23427 ms in the other one which clearly depicts that in-mapper combining with state preserved across documents is more efficient in mapping than not preserving states in terms of time complexity.


2. Time Spent in Reduce Tasks


The total time spent in all reduce task while preserving state is 18613 ms while it is 23391 ms in the other one which clearly depicts that in-mapper combining with state preserved across documents is more efficient in reducing than not preserving states in terms of time complexity.


Therefore, in the whole process of map-reduce, in-mapper combining with state preserved across states is more efficient than not preserving states.

3. Other Parameters


The values calculated below may vary with different computational capactities. I am running the tests on Ubuntu 20.04 using a PC with an i7 8th gen processor, also equipped with GTX 1050.

---
Not Preserved     			| 	Preserved State			 |
---
Total mb-ms by map tasks = 38983872	|	Total mb-ms by map tasks = 28136448

Total mb-ms by reduce tasks = 71857152 |	Total mb-ms by reduce tasks = 57179136

Map output records = 631806	        |	Map output records = 6209

Total Committed heap usage= 1757413376 |	Total Committed heap usage = 1824522240

Spilled Records = 1263612 records	|	Spilled Records = 12418 records

CPU time spent = 12200 		|	CPU time spent = 5150 ms
---

The total mb-ms while preserving state is less than while not preserving space which makes it more efficient in both map and reduce tasks in terms and time and space used. The latter parameter space is also used less by Preserved State in this case as depicted by the total committed heap usage which is less in preserved state as compared when not preserving states.

The map output records were less in preserved state than not preserved which means that less key-value pairs or records were passed to the reducer making it quicker to get the job done and making it more efficient.

As the map output records were less which means the need to reduce data is decreased, we can see that the number of records spilled while reducing is less in preserving state, as while preserving state we already have the previous data stored which can be incremented if the new key is already present in the associative array which is not possible while not preserving state. This makes preserving state more efficient than not preserving state.

while preserving state, we decrease the time in which the CPU has to be utilized to carry out the map-reduce process by more than half than while not preserving state. This factor becomes very important as in case of using "Pay as you go", we will save financial resources as less CPU time is spent.

To conclude, the preserved state approach may take require more space (or total committed heap usage) but is efficient in most of the other factors which makes it more favored than in-mapper combining without preserving state.


### File Structure
---

Folder: ./jar
It contains the jar files required to execute the tasks mentioned above.

Folder: ./src/MapAndReduce
It contains the project folder required to execute the task of getting the word length and testing different approaches like preserving document, in-mapper combining etc. with map reduce.

Folder: ./src/WordLengthCategorizer
It contains the project folder required to perform the task of counting the pair of words.

Folder: ./src/WordPairCounter2
It contains the project folder required to perform the task of calculating the relative frequency of the word pairs.

Folder: ./data
It contains the data files used to perform the map reduce operations.
 
