Huahin Manager

Huahin Manager is a Simple Management System for Hadoop MapReduce Job.
Huahin can get a list of MapReduce jobs, get status, do a kill for the job, and Job queue management.

Huahin Manager is distributed under Apache License 2.0.


-----------------------------------------------------------------------------
Documentation
  http://huahinframework.org/huahin-manager/

-----------------------------------------------------------------------------
Requirements
  * Java 6+

-----------------------------------------------------------------------------
Install Huahin Manager
  ~ $ tar xzf huahin-manager-x.x.x.tar.gz

-----------------------------------------------------------------------------
Configure Huahin Manager

Edit the huahin-manager-x.x.x/conf/huahinManager.properties file and set mapred.job.tracker property to the JobTracker URI,
set fs.default.name property to the NameNode URI, and set job.queue.limit property to the job queue limit.
job queue limit is 0, does not manage the queue.
For example:

  mapred.job.tracker=localhost:9001
  fs.default.name=hdfs://localhost:9000
  hiveserver=localhost:10000 # option
  job.queue.limit=2

When you change the boot port, edit the huahin-manager-x.x.x/conf/port file.

-----------------------------------------------------------------------------
Start/Stop Huahin Manager

To start/stop Huahin Manager use Huahin Manager's bin/manager script. For example:

  $ bin/manager start

-----------------------------------------------------------------------------
Test Huahin Manager is working

  ~ $ curl -X GET "http://<HOSTNAME>:9010/job/list"
[
  {
    "jobid": "job_201205111223_0001",
    "mapComplete": "100.0%",
    "name": "JOB_EBCF7626A41F34B4C7276DB2B152336F",
    "priority": "NORMAL",
    "reduceComplete": "100.0%",
    "schedulingInfo": "NA",
    "startTime": "Fri May 11 12:25:18 JST 2012",
    "state": "SUCCEEDED",
    "user": "huahin"
  }
]

-----------------------------------------------------------------------------
Huahin Manager REST Job APIs

Get all job list.
  For example:
  ~ $ curl -X GET "http://<HOSTNAME>:9010/job/list"

Get failed job list.
  For example:
  ~ $ curl -X GET "http://<HOSTNAME>:9010/job/list/failed"

Get killed job list.
  For example:
  ~ $ curl -X GET "http://<HOSTNAME>:9010/job/list/killed"

Get prep job list.
  For example:
  ~ $ curl -X GET "http://<HOSTNAME>:9010/job/list/prep"

Get running job list.
  For example:
  ~ $ curl -X GET "http://<HOSTNAME>:9010/job/list/running"

Get succeeded job list.
  For example:
  ~ $ curl -X GET "http://<HOSTNAME>:9010/job/list/succeeded"

Get job status.
  <JOBID> specifies the jobid.
  ~ $ curl -X GET "http://<HOSTNAME>:9010/job/status/<JOBID>"

  For example:
  ~ $ curl -X GET "http://<HOSTNAME>:9010/job/status/job_201205111223_0001"

Get job detail.
  <JOBID> specifies the jobid.
  ~ $ curl -X GET "http://<HOSTNAME>:9010/job/detail/<JOBID>"

  For example:
  ~ $ curl -X GET "http://<HOSTNAME>:9010/job/detail/job_201205111223_0001"

Register job
  JAR=@<JAR_FILE> specifies the run jar file.
  ARGUMENTS specifies the JSON. <CLASS> specifies the run class. arguments:<ARGS> specifies the run arguments array.
  ~ $ curl -X POST "http://<HOSTNAME>:9010/job/register -F JAR=@<JAR_FILE> -F ARGUMENTS='{"class":"<CLASS>","arguments":["<ARGS>","<ARGS>"]}'

  For example:
  ~ $ curl -X POST "http://<HOSTNAME>:9010/job/register -F JAR=@mapreduce.jar -F ARGUMENTS='{"class":"examples.WordCount","arguments":["/user/huahin/input","/user/huahin/output"]}'

Register Hive job
  Because they are executed in the queue, the return value must be a table or HDFS.
  ARGUMENTS specifies the JSON. <script> specifies the Hive query.
  ~ $ curl -X POST "http://<HOSTNAME>:9010/job/hive/register -F ARGUMENTS='{"script":"<script>"}'

  For example:
  ~ $ curl -X POST "http://<HOSTNAME>:9010/job/hive/register \
  -F ARGUMENTS='{"script":"insert overwrite directory '\''/tmp/out'\'' select word, count(word) as cnt from words group by word"}'

Register Pig job
  Because they are executed in the queue, the return value must be a table or HDFS.
  ARGUMENTS specifies the JSON. <script> specifies the Pig Latin.
  ~ $ curl -X POST "http://<HOSTNAME>:9010/job/pig/register -F ARGUMENTS='{"script":"<script>"}'

  For example:
  ~ $ curl -X POST "http://<HOSTNAME>:9010/job/pig/register \
  -F ARGUMENTS='{"script":"a = load '\''/user/huahin/input'\'' as (text:chararray);b = foreach a generate flatten(TOKENIZE(text)) as word;c = group b by word;d = foreach c generate group as word, COUNT(b) as count;store d into '\''/tmp/out'\'';"}'

Kill job for ID.
  <JOBID> specifies the job ID.
  ~ $ curl -X DELETE "http://<HOSTNAME>:9010/job/kill/id/<JOBID>"

  For example:
  ~ $ curl -X DELETE "http://<HOSTNAME>:9010/job/kill/id/job_201205111223_0001"

Kill job for job name.
  <JOBNAME> specifies the job name.
  ~ $ curl -X DELETE "http://<HOSTNAME>:9010/job/kill/name/<JOBNAME>"

  For example:
  ~ $ curl -X DELETE "http://<HOSTNAME>:9010/job/kill/name/WORD_COUNT_JOB"

-----------------------------------------------------------------------------
Huahin Manager REST queue APIs

Get all queue list.
  For example:
  ~ $ curl -X GET "http://<HOSTNAME>:9010/queue/list"

Get run queue statuses.
  For example:
  ~ $ curl -X GET "http://<HOSTNAME>:9010/queue/statuses"

Kill queue for ID.
  <QUEUEID> specifies the queue ID.
  ~ $ curl -X DELETE "http://<HOSTNAME>:9010/queue/kill/<QUEUEID>"

  For example:
  ~ $ curl -X DELETE "http://<HOSTNAME>:9010/queue/kill/Q_20120608180129594"

-----------------------------------------------------------------------------
Huahin Manager REST Hive APIs

Execution of the query does not return value
  ARGUMENTS specifies the JSON. <query> specifies the hive query.
  ~ $ curl -X POST "http://<HOSTNAME>:9010/hive/execute -F ARGUMENTS='{"query":"<query>"}'

  For example:
  ~ $ curl -X POST "http://<HOSTNAME>:9010/hive/execute \
  -F ARGUMENTS='{"query":"create table foo(bar string)"}'

Query execution with return value
  The return value is returned in the stream.
  ARGUMENTS specifies the JSON. <query> specifies the hive query.
  ~ $ curl -X POST "http://<HOSTNAME>:9010/hive/executeQuery -F ARGUMENTS='{"query":"<query>"}'

  For example:
  ~ $ curl -X POST "http://<HOSTNAME>:9010/hive/executeQuery \
  -F ARGUMENTS='{"query":"select word, count(word) as cnt from words group by word"}'

-----------------------------------------------------------------------------
Huahin Manager REST Pig APIs

Execution of the dump
  ARGUMENTS specifies the JSON. <variable> is that specifies the dump. <query> specifies the Pig Latin.
  ~ $ curl -X POST "http://<HOSTNAME>:9010/pig/dump -F ARGUMENTS='{"dump":"<variable>","query":"<query>"}'

  For example:
  ~ $ curl -X POST "http://<HOSTNAME>:9010/pig/dump \
  -F ARGUMENTS='{"dump":"d","query":"a = load '\''/user/huahin/input'\'' as (text:chararray);b = foreach a generate flatten(TOKENIZE(text)) as word;c = group b by word;d = foreach c generate group as word, COUNT(b) as count;"}'

Execution of the store
  The return value is returned in the stream.
  ARGUMENTS specifies the JSON. <query> specifies the Pig Latin.
  ~ $ curl -X POST "http://<HOSTNAME>:9010/pig/store -F ARGUMENTS='{"query":"<query>"}'

  For example:
  ~ $ curl -X POST "http://<HOSTNAME>:9010/pig/store \
  -F ARGUMENTS='{"query":"a = load '\''/user/huahin/input'\'' as (text:chararray);b = foreach a generate flatten(TOKENIZE(text)) as word;c = group b by word;d = foreach c generate group as word, COUNT(b) as count;store d into '\''/tmp/out'\'';"}'
