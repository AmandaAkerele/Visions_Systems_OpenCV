I wanted to add to what Marcus sent in his email. 
The GUD file for the indicators is a huge file and cannot accommodate the use of Pandas Library in Python. I also believe the DAD and NACRS Enriched file is also a huge file similar to the GUD File(please correct me if i am wrong). 
If the Enriched file is as huge as the GUD file, pyspark will be most sufficient for use as pandas cannot be used to handle such big files. 
