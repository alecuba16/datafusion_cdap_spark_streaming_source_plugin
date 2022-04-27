package com.alecuba16.cdap;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.Status;
import twitter4j.auth.Authorization;
import twitter4j.auth.OAuthAuthorization;
import twitter4j.conf.ConfigurationBuilder;
import java.io.Serializable;

/**
 * Streaming Source Plugin
 * A Streaming Source plugin is used as a source in real-time data pipelines. It is used to fetch a Spark DStream, which is an object that represents a collection of Spark RDDs and that produces a new RDD every batch interval of the pipeline.
 *
 * In order to implement a Streaming Source Plugin, you extend the StreamingSource class.
 *
 * Methods
 *  configurePipeline():
 * Used to perform any validation on the application configuration that is required by this plugin or to create any datasets if the fieldName for a dataset is not a macro.
 * getStream():
 * Returns the JavaDStream that will be used as a source in the pipeline.
 */

@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name("Twitter")
@Description("Twitter streaming source.")
public class TwitterStreamingSource extends StreamingSource<StructuredRecord> {
    private final TwitterStreamingConfig config;

    /**
     * Config class for TwitterStreamingSource.
     */
    public static class TwitterStreamingConfig extends PluginConfig implements Serializable {
        private static final long serialVersionUID = 4218063781909515444L;

        private String consumerKey;

        private String consumerSecret;

        private String accessToken;

        private String accessTokenSecret;

        private String referenceName;
    }

    public TwitterStreamingSource(TwitterStreamingConfig config) {
        this.config = config;
    }

    @Override
    public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
        pipelineConfigurer.getStageConfigurer().setOutputSchema(null);
    }

    @Override
    public JavaDStream<StructuredRecord> getStream(StreamingContext context) throws Exception {
        JavaStreamingContext javaStreamingContext = context.getSparkStreamingContext();
        // lineage for this source will be tracked with this reference name
        context.registerLineage(config.referenceName);

        // Create authorization from user-provided properties
        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.setDebugEnabled(false)
                .setOAuthConsumerKey(config.consumerKey)
                .setOAuthConsumerSecret(config.consumerSecret)
                .setOAuthAccessToken(config.accessToken)
                .setOAuthAccessTokenSecret(config.accessTokenSecret);
        Authorization authorization = new OAuthAuthorization(configurationBuilder.build());
        return TwitterUtils.createStream(javaStreamingContext, authorization).map(
                new Function<Status, StructuredRecord>() {
                    public StructuredRecord call(Status status) {
                        return convertTweet(status);
                    }
                }
        );
    }

    private StructuredRecord convertTweet(Status tweet) {
        // logic to convert a Twitter Status into a CDAP StructuredRecord
        return null;
    }

}