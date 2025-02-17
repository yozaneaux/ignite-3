/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Utility class to read Ignite properties from properties file.
 */
public class IgniteProperties {
    /** Properties file path. */
    private static final String FILE_PATH = "ignite.properties";

    /** Properties. */
    private static final Properties PROPS;

    static {
        PROPS = new Properties();

        readProperties(FILE_PATH, PROPS, true);
    }

    /**
     * Reads properties.
     *
     * @param path     Path.
     * @param props    Properties.
     * @param throwExc Flag indicating whether to throw an exception or not.
     */
    public static void readProperties(String path, Properties props, boolean throwExc) {
        try (InputStream is = IgniteProperties.class.getClassLoader().getResourceAsStream(path)) {
            if (is == null) {
                if (throwExc) {
                    throw new RuntimeException("Failed to find properties file: " + path);
                } else {
                    return;
                }
            }

            props.load(is);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read properties file: " + path, e);
        }
    }

    /**
     * Returns property value for a given key or {@code null} if nothing was found.
     *
     * @param key Key.
     * @return Value or {@code null}.
     */
    public static String get(String key) {
        return PROPS.getProperty(key, "");
    }

    /**
     * Constructor.
     */
    private IgniteProperties() {
        // No-op.
    }
}
