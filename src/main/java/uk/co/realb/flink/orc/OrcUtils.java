package uk.co.realb.flink.orc;

import org.apache.hadoop.conf.Configuration;

import java.util.Properties;

public class OrcUtils {
    public static Configuration getConfiguration(Properties props) {
        Configuration config = new Configuration();
        props.stringPropertyNames()
                .stream()
                .filter(p -> props.getProperty(p) != null)
                .forEach(p -> config.set(p, props.getProperty(p)));
        return config;
    }
}
