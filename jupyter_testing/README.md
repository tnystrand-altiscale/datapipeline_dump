# Steps:

1) `source activate py27`
2) `cd semi_serious/jupyter_testing`
3) `export SPARK_HOME='/opt/alti-spark-2.1.1/'`
4) `jupyter lab`

# Toughts:
* Could easily extend it with pdb magic wrappers and give this type of info in UI

---
# sql_magic
*SQL magic features:*
* Browser notifications
* Asynchronous execution
* Returns panda dataframes for quick and easy plotting
* We could create code that intercepts this output
** Javascripting in jupyter notebook
** npm in jupyter lab


*Issues with sql_magic:*
* No query runtime updates
* -Unhelpful- (non-business) error messages
* Had to restart kernal when OOM
* Seems to only work with spark>2.0 (despite documentation)
* Needs thriftserver...
* No widget javascript output in jupyter lab
* No direct graphs -> code needed
* No dependencies from async queries. Must wait -> then run to get graph

*Solutions:*
* Write wrappers around sql_magic which pings spark_ui for status updates
* Write wrappers around sql_magic run commands that checks against OOM and tells user to restart kernel
* Write wrappers around sql_magic that plots output

E.g. OOM (figured from kernel error messages):
```---------------------------------------------------------------------------
Py4JJavaError                             Traceback (most recent call last)
<ipython-input-12-465a54f05491> in <module>()
----> 1 get_ipython().magic(u'read_sql select count(*), partition_date from burst_time_series_patchjoin group by partition_date')

/home/tnystrand/anaconda2/lib/python2.7/site-packages/IPython/core/interactiveshell.pyc in magic(self, arg_s)
   2334         magic_name, _, magic_arg_s = arg_s.partition(' ')
   2335         magic_name = magic_name.lstrip(prefilter.ESC_MAGIC)
-> 2336         return self.run_line_magic(magic_name, magic_arg_s)
   2337 
   2338     #-------------------------------------------------------------------------

/home/tnystrand/anaconda2/lib/python2.7/site-packages/IPython/core/interactiveshell.pyc in run_line_magic(self, magic_name, line)
   2255                 kwargs['local_ns'] = sys._getframe(stack_depth).f_locals
   2256             with self.builtin_trap:
-> 2257                 result = fn(*args,**kwargs)
   2258             return result
   2259 

<decorator-gen-123> in read_sql(self, line, cell, local_ns)

/home/tnystrand/anaconda2/lib/python2.7/site-packages/IPython/core/magic.pyc in <lambda>(f, *a, **k)
    191     # but it's overkill for just that one bit of state.
    192     def magic_deco(arg):
--> 193         call = lambda f, *a, **k: f(*a, **k)
    194 
    195         if callable(arg):

/home/tnystrand/anaconda2/lib/python2.7/site-packages/sql_magic/sql_magic.pyc in read_sql(self, line, cell, local_ns)
    141             t.start()
    142         else:
--> 143             result = self.conn.execute_sqls(statements, options)
    144             if options['display']:
    145                 return result

/home/tnystrand/anaconda2/lib/python2.7/site-packages/sql_magic/connection.pyc in execute_sqls(self, sqls, options)
    144         r = None
    145         for i, s in enumerate(sqls, start=1):
--> 146             r = self._read_sql_engine(s, options)
    147         return r  # return last result

/home/tnystrand/anaconda2/lib/python2.7/site-packages/sql_magic/connection.pyc in _read_sql_engine(self, sql, options)
    116         if caller is None:
    117             raise ConnectionNotConfigured("A connection object must be configured using %config SQL.conn_name")
--> 118         result, del_time, time_output = self._time_and_run_query(caller, sql)
    119 
    120         if table_name:

/home/tnystrand/anaconda2/lib/python2.7/site-packages/sql_magic/connection.pyc in _time_and_run_query(self, caller, sql)
    132         sys.stdout.write(time_output)
    133         start_time = time.time()
--> 134         result = caller(sql)
    135         end_time = time.time()
    136         del_time = (end_time - start_time) / 60.

/home/tnystrand/anaconda2/lib/python2.7/site-packages/sql_magic/connection.pyc in _run_spark_sql(sql_code)
     77                 tokens = tokens[:-1]
     78                 sql_code = (''.join([t.value for t in tokens]))
---> 79             df = conn_object.sql(sql_code).toPandas()
     80             if df.shape == (0, 0):
     81                 return EmptyResult()

/opt/alti-spark-2.1.1/python/pyspark/sql/dataframe.py in toPandas(self)
   1583         """
   1584         import pandas as pd
-> 1585         return pd.DataFrame.from_records(self.collect(), columns=self.columns)
   1586 
   1587     ##########################################################################################

/opt/alti-spark-2.1.1/python/pyspark/sql/dataframe.py in collect(self)
    389         """
    390         with SCCallSiteSync(self._sc) as css:
--> 391             port = self._jdf.collectToPython()
    392         return list(_load_from_socket(port, BatchedSerializer(PickleSerializer())))
    393 

/opt/alti-spark-2.1.1/python/lib/py4j-0.10.4-src.zip/py4j/java_gateway.py in __call__(self, *args)
   1131         answer = self.gateway_client.send_command(command)
   1132         return_value = get_return_value(
-> 1133             answer, self.gateway_client, self.target_id, self.name)
   1134 
   1135         for temp_arg in temp_args:

/opt/alti-spark-2.1.1/python/pyspark/sql/utils.py in deco(*a, **kw)
     61     def deco(*a, **kw):
     62         try:
---> 63             return f(*a, **kw)
     64         except py4j.protocol.Py4JJavaError as e:
     65             s = e.java_exception.toString()

/opt/alti-spark-2.1.1/python/lib/py4j-0.10.4-src.zip/py4j/protocol.py in get_return_value(answer, gateway_client, target_id, name)
    317                 raise Py4JJavaError(
    318                     "An error occurred while calling {0}{1}{2}.\n".
--> 319                     format(target_id, ".", name), value)
    320             else:
    321                 raise Py4JError(

Py4JJavaError: An error occurred while calling o96.collectToPython.
: org.apache.spark.SparkException: Job 0 cancelled because SparkContext was shut down
    at org.apache.spark.scheduler.DAGScheduler$$anonfun$cleanUpAfterSchedulerStop$1.apply(DAGScheduler.scala:808)
    at org.apache.spark.scheduler.DAGScheduler$$anonfun$cleanUpAfterSchedulerStop$1.apply(DAGScheduler.scala:806)
    at scala.collection.mutable.HashSet.foreach(HashSet.scala:78)
    at org.apache.spark.scheduler.DAGScheduler.cleanUpAfterSchedulerStop(DAGScheduler.scala:806)
    at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onStop(DAGScheduler.scala:1668)
    at org.apache.spark.util.EventLoop.stop(EventLoop.scala:83)
    at org.apache.spark.scheduler.DAGScheduler.stop(DAGScheduler.scala:1587)
    at org.apache.spark.SparkContext$$anonfun$stop$8.apply$mcV$sp(SparkContext.scala:1833)
    at org.apache.spark.util.Utils$.tryLogNonFatalError(Utils.scala:1283)
    at org.apache.spark.SparkContext.stop(SparkContext.scala:1832)
    at org.apache.spark.SparkContext$$anon$3.run(SparkContext.scala:1777)
    at org.apache.spark.scheduler.DAGScheduler.runJob(DAGScheduler.scala:628)
    at org.apache.spark.SparkContext.runJob(SparkContext.scala:1925)
    at org.apache.spark.SparkContext.runJob(SparkContext.scala:1938)
    at org.apache.spark.SparkContext.runJob(SparkContext.scala:1951)
    at org.apache.spark.SparkContext.runJob(SparkContext.scala:1965)
    at org.apache.spark.rdd.RDD$$anonfun$collect$1.apply(RDD.scala:936)
    at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:151)
    at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:112)
    at org.apache.spark.rdd.RDD.withScope(RDD.scala:362)
    at org.apache.spark.rdd.RDD.collect(RDD.scala:935)
    at org.apache.spark.sql.execution.SparkPlan.executeCollect(SparkPlan.scala:275)
    at org.apache.spark.sql.Dataset$$anonfun$collectToPython$1.apply$mcI$sp(Dataset.scala:2760)
    at org.apache.spark.sql.Dataset$$anonfun$collectToPython$1.apply(Dataset.scala:2757)
    at org.apache.spark.sql.Dataset$$anonfun$collectToPython$1.apply(Dataset.scala:2757)
    at org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:57)
    at org.apache.spark.sql.Dataset.withNewExecutionId(Dataset.scala:2780)
    at org.apache.spark.sql.Dataset.collectToPython(Dataset.scala:2757)
    at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
    at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
    at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
    at java.lang.reflect.Method.invoke(Method.java:498)
    at py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:244)
    at py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:357)
    at py4j.Gateway.invoke(Gateway.java:280)
    at py4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132)
    at py4j.commands.CallCommand.execute(CallCommand.java:79)
    at py4j.GatewayConnection.run(GatewayConnection.java:214)
    at java.lang.Thread.run(Thread.java:748)```
