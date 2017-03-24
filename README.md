#### Windowing In Spark
This is an example of how to use windowing with spark datasets. 
To execute from the command line:

$> sbt run

The joins 2 datasets, events and impressions. Then finds duplicates.
If two click events are within 60 seconds, they considered duplicates.

