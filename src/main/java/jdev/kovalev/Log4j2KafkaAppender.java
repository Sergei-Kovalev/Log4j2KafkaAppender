package jdev.kovalev;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;

import java.io.Serializable;
import java.util.Properties;

@Plugin(name = "Kafka", category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE, printObject = true)
public class Log4j2KafkaAppender extends AbstractAppender {

    private final Producer<String, String> producer;
    private final String topic;

    protected Log4j2KafkaAppender(String name, Filter filter, Layout<? extends Serializable> layout,
                                  boolean ignoreExceptions, String topic, Properties kafkaProperties) {
        super(name, filter, layout, ignoreExceptions);
        this.producer = createProducer(kafkaProperties);
        this.topic = topic;
    }

    @Override
    public void append(LogEvent event) {
        String message = new String(getLayout().toByteArray(event));
        producer.send(new ProducerRecord<>(topic, message), (metadata, exception) -> {
            if (exception != null) {
                LOGGER.error("Failed to send log to Kafka: {}", exception.getMessage());
            }
        });
    }

    @Override
    public void stop() {
        super.stop();
        if (producer != null) {
            producer.flush();
            producer.close();
        }
    }

    @PluginFactory
    public static Log4j2KafkaAppender createAppender(
            @PluginAttribute("name") String name,
            @PluginAttribute("topic") String topic,
            @PluginElement("Filter") Filter filter,
            @PluginElement("Layout") Layout<? extends Serializable> layout,
            @PluginAttribute(value = "ignoreExceptions", defaultBoolean = true) boolean ignoreExceptions,
            @PluginElement("Properties") Property[] properties) {

        if (name == null) {
            LOGGER.error("No name provided for KafkaAppender");
            return null;
        }

        if (topic == null) {
            LOGGER.error("No topic provided for KafkaAppender");
            return null;
        }

        if (layout == null) {
            layout = PatternLayout.createDefaultLayout();
        }

        Properties kafkaProps = getProperties(properties);

        if (!kafkaProps.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
            LOGGER.error("No bootstrap servers configured for KafkaAppender");
            return null;
        }

        return new Log4j2KafkaAppender(name, filter, layout, ignoreExceptions, topic, kafkaProps);
    }

    private static Properties getProperties(Property[] properties) {
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                       "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                       "org.apache.kafka.common.serialization.StringSerializer");

        if (properties != null) {
            for (Property property : properties) {
                kafkaProps.setProperty(property.getName(), property.getValue());
            }
        }
        return kafkaProps;
    }

    private Producer<String, String> createProducer(Properties kafkaProps) {
        return new KafkaProducer<>(kafkaProps);
    }
}
