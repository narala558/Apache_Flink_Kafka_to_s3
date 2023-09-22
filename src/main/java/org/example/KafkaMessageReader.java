package org.example;

import com.amazonaws.services.s3.model.ObjectMetadata;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;
import java.util.UUID;

public class KafkaMessageReader {

    private static final int EVENTS_PER_FILE = 5; // Number of events per file
    private static final int MAX_EVENTS_IN_BUFFER = 10; // Maximum events to accumulate before upload

    public static void main(String[] args) throws Exception {
        // Set up the Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Define the Kafka properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "Hostname:9092"); // Kafka broker address
        properties.setProperty("group.id", "vasanth_testing"); // Consumer group ID

        // Create a Kafka consumer for reading messages
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "test.purchases", // Kafka topic to read from
                new SimpleStringSchema(),
                properties
        );

        // Add the Kafka source to the Flink environment
        DataStream<String> kafkaStream = env.addSource(kafkaConsumer);

        // Define your S3 bucket name
        String s3BucketName = "BucketName/flink-test/";

        // Write the Kafka stream data to S3 with date and hour-based folder structure
        kafkaStream.addSink(new S3SinkFunction(s3BucketName));

        // Execute the Flink job
        env.execute("Kafka Message Reader");
    }

    static class S3SinkFunction extends RichSinkFunction<String> {
        private final String s3BucketName;
        private transient AmazonS3 s3Client; // Make it transient to avoid serialization
        private StringBuilder buffer = new StringBuilder(); // Buffer for storing events
        private int eventCount = 0;

        public S3SinkFunction(String s3BucketName) {
            this.s3BucketName = s3BucketName;
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            super.open(parameters);

            // Initialize the S3 client here, which is not serializable
            s3Client = AmazonS3ClientBuilder.standard()
                    .withCredentials(new AWSStaticCredentialsProvider(
                            new BasicAWSCredentials("****************", "**************")))
                    .build();
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            // Append the event to the buffer
            buffer.append(value).append("\n");
            eventCount++;

            // Check if we've reached the desired number of events or the buffer size limit
            if (eventCount >= EVENTS_PER_FILE || buffer.length() >= MAX_EVENTS_IN_BUFFER) {
                // Generate the object key with the current date, timestamp, and a unique identifier
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd/HH");
                sdf.setTimeZone(TimeZone.getTimeZone("Asia/Kolkata")); // Set timezone to IST
                String currentFolder = sdf.format(new Date());
                String timestamp = new SimpleDateFormat("yyyyMMdd-HHmmss").format(new Date());
                String fileId = UUID.randomUUID().toString(); // Unique identifier
                String s3ObjectKey = currentFolder + "/" + timestamp + "-" + fileId + ".txt";

                // Upload the buffer contents to S3 using the initialized S3 client
                byte[] dataBytes = buffer.toString().getBytes();
                InputStream inputStream = new ByteArrayInputStream(dataBytes);

                // Set the content length for the object
                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setContentLength(dataBytes.length);

                s3Client.putObject(s3BucketName, s3ObjectKey, inputStream, metadata);

                // Reset the buffer and event count
                buffer.setLength(0);
                eventCount = 0;
            }
        }
    }
}
