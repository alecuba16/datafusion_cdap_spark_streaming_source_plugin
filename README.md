# datafusion_cdap_spark_streaming_source_plugin
Complete example project to create a custom Google cloud datafusion (CDAP) spark streaming source plugin. Sourced and adapted from the documentation where there is no quickstart project.
## Streaming Source Plugin
A Streaming Source plugin is used as a source in real-time data pipelines. It is used to fetch a Spark DStream, which is an object that represents a collection of Spark RDDs and that produces a new RDD every batch interval of the pipeline.

In order to implement a Streaming Source Plugin, you extend the StreamingSource class.

##Methods
### configurePipeline():
Used to perform any validation on the application configuration that is required by this plugin or to create any datasets if the fieldName for a dataset is not a macro.
### getStream():
Returns the JavaDStream that will be used as a source in the pipeline.

