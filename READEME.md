Welcome to Project Athena
==========================



Authors
=======
Mehrdad
Hayden


Installation
===============


Kafka:
-------





Spark:
--------

Pass Python virtual env to PYSPARK_PYTHON on shell before running the job.
http://mail-archives.us.apache.org/mod_mbox/spark-user/201403.mbox/%3CCAG-p0g2L=z9H1H4ZY1XdLOGnGyPEKqi8+=tPiEqVDwtVwwAZWg@mail.gmail.com%3E

Run the job:
export PYSPARK_PYTHON=/var/athena/athena_venv/bin/python; ./spark-submit --jars ../../lib/spark-streaming-kafka-assembly_2.10-1.5.1.jar --master spark://athena-master01:7077 /var/athena/Athena/kafka_streaming.py