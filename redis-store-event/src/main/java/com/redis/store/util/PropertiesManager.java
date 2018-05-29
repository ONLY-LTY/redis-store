package com.redis.store.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.io.InputStream;
import java.util.*;

/**
 * Author LTY
 * Date 2018/05/25
 */
public class PropertiesManager {
    private static final Logger LOGGER = Logger.getLogger(PropertiesManager.class);
    private Properties props;

    public PropertiesManager(InputStream inputStream) {
        try {
            this.props = new Properties();
            this.props.load(inputStream);
        } catch (Exception var3) {
            LOGGER.error("Error loading input stream", var3);
        }

    }

    public PropertiesManager(String propertiesFilePath) {
        try {
            this.props = new Properties();
            this.props.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(propertiesFilePath));
        } catch (Exception var3) {
            LOGGER.error("Error loading properties, filepath: " + propertiesFilePath, var3);
        }

    }

    public PropertiesManager(Map<String, Object> map) {
        this.props = new Properties();
        this.props.putAll(map);
    }

    public boolean hasProperty(String key) {
        return this.props.containsKey(key);
    }

    public String getProperty(String key) {
        return this.props.getProperty(key);
    }

    public int getIntProperty(String key) {
        if (this.hasProperty(key)) {
            String value = this.getProperty(key);
            return Integer.parseInt(value);
        } else {
            return 0;
        }
    }

    public String getProperty(String key, String defaultValue) {
        return this.hasProperty(key) ? this.getProperty(key) : defaultValue;
    }

    public int getIntProperty(String key, int defaultValue) {
        if (this.hasProperty(key)) {
            String value = this.getProperty(key);
            return Integer.parseInt(value);
        } else {
            return defaultValue;
        }
    }

    public long getLongProperty(String key, long defaultValue) {
        if (this.hasProperty(key)) {
            String value = this.getProperty(key);
            return Long.parseLong(value);
        } else {
            return defaultValue;
        }
    }

    public boolean getBooleanProperty(String key, boolean defaultValue) {
        if (this.hasProperty(key)) {
            String value = this.getProperty(key);
            return Boolean.parseBoolean(value);
        } else {
            return defaultValue;
        }
    }

    public float getFloatProperty(String key, float defaultValue) {
        if (this.hasProperty(key)) {
            String value = this.getProperty(key);
            return Float.parseFloat(value);
        } else {
            return defaultValue;
        }
    }

    public Map<String, Object> getPropsByPrefix(String prefix) {
        return this.getPropertiesByPrefix(prefix, true);
    }

    public Map<String, Object> getPropsByPrefix(String prefix, boolean isNodeBelongedPrefix) {
        return StringUtils.isEmpty(prefix) ? this.getPropertiesByPrefix(prefix, true) : this.getPropertiesByPrefix(prefix, isNodeBelongedPrefix);
    }

    private Map<String, Object> getPropertiesByPrefix(String prefix, boolean isNodeBelongedPrefix) {
        Map<String, Object> properties = new HashMap();
        Set<Object> keys = this.props.keySet();
        Iterator var5 = keys.iterator();

        while (true) {
            String propKey;
            String completePrefix;
            do {
                do {
                    if (!var5.hasNext()) {
                        return properties;
                    }

                    Object key = var5.next();
                    propKey = key + "";
                } while (!propKey.startsWith(prefix));

                completePrefix = "";
                if (isNodeBelongedPrefix) {
                    completePrefix = this.getCompletePrefix(prefix, propKey);
                    break;
                }

                completePrefix = this.getNextPrefix(prefix, propKey);
            } while (StringUtils.isEmpty(completePrefix));

            if (propKey.contains(completePrefix)) {
                String node = this.getNode(completePrefix, completePrefix, true);
                if (completePrefix.equals(propKey)) {
                    properties.put(node, this.getProperty(completePrefix));
                } else {
                    properties.put(node, this.getPropertiesByPrefix(completePrefix, false));
                }
            }
        }
    }

    private String getCompletePrefix(String prefix, String key) {
        if (!key.contains(prefix)) {
            return null;
        } else {
            String[] parts = key.split("\\.");
            String completePrefix = null;
            int len = prefix.length();
            if (prefix.endsWith(".")) {
                --len;
            }

            for (int i = 0; i < parts.length; ++i) {
                len -= parts[i].length();
                if (len <= 0) {
                    try {
                        StringBuffer sb = new StringBuffer();

                        for (int j = 0; j <= i; ++j) {
                            sb.append(parts[j]);
                            if (j != i) {
                                sb.append(".");
                            }
                        }

                        completePrefix = sb.toString();
                        break;
                    } catch (Exception var9) {
                        LOGGER.error("getCompletePrefix error , please check your prefix " + new Object[0], var9);
                        return null;
                    }
                }

                --len;
            }

            return completePrefix;
        }
    }

    private String getNextPrefix(String completePrefix, String key) {
        if (!key.contains(completePrefix + ".")) {
            return null;
        } else {
            String[] parts = key.split("\\.");
            String nexPrefix = null;
            int len = completePrefix.length();

            for (int i = 0; i < parts.length; ++i) {
                len -= parts[i].length();
                if (len <= 0) {
                    StringBuffer sb = new StringBuffer();

                    for (int j = 0; j <= i + 1; ++j) {
                        sb.append(parts[j]);
                        if (j != i + 1) {
                            sb.append(".");
                        }
                    }

                    nexPrefix = sb.toString();
                    break;
                }

                --len;
            }

            return nexPrefix;
        }
    }

    private Set<String> getGroups(String prefix, boolean isNodeBelongedPrefix) {
        Set<String> propertyWords = new HashSet();
        Set<Object> keys = this.props.keySet();
        Iterator var5 = keys.iterator();

        while (var5.hasNext()) {
            Object key = var5.next();
            if ((key + "").startsWith(prefix)) {
                propertyWords.add(this.getNode(prefix, key + "", isNodeBelongedPrefix));
            }
        }

        return propertyWords;
    }

    public Set<String> getGroups(String prefix) {
        return this.getGroups(prefix, true);
    }

    private String getNode(String prefix, String key, boolean isNodeBelongPrefix) {
        if (!key.contains(prefix)) {
            return null;
        } else {
            String[] parts = key.split("\\.");
            String group = null;
            int len = prefix.length();
            if (prefix.endsWith(".")) {
                --len;
            }

            for (int i = 0; i < parts.length; ++i) {
                len -= parts[i].length();
                if (len <= 0) {
                    if (isNodeBelongPrefix) {
                        group = parts[i];
                    } else {
                        if (i + 1 == parts.length) {
                            return null;
                        }

                        group = parts[i + 1];
                    }
                    break;
                }

                --len;
            }

            return group;
        }
    }
}
