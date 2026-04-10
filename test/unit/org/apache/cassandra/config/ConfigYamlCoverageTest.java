/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.config;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

import org.yaml.snakeyaml.introspector.Property;

import static org.junit.Assert.assertTrue;

/**
 * Verifies that every non-static field in {@link Config} is documented in at least one of
 * {@code cassandra.yaml} or {@code cassandra_latest.yaml}, unless explicitly marked with
 * {@link Hidden} or {@link Deprecated}.
 * <p>
 * This test catches accidental omissions when new configuration properties are added to
 * {@link Config} without a corresponding entry in the yaml templates.
 */
public class ConfigYamlCoverageTest
{
    private static final Path CONF_DIR = Paths.get("conf");

    // Matches top-level yaml keys: both uncommented ("key:") and commented ("# key:")
    private static final Pattern YAML_KEY_PATTERN = Pattern.compile("^#?\\s*(\\w+)\\s*:", Pattern.MULTILINE);

    @Test
    public void allConfigFieldsShouldBeDocumentedInYaml() throws IOException
    {
        Set<String> yamlKeys = extractYamlKeys();
        Map<String, Property> configProperties = Properties.defaultLoader().getProperties(Config.class);
        Set<String> undocumented = new HashSet<>();

        for (String name : configProperties.keySet())
        {
            if (isExcluded(name))
                continue;

            if (!yamlKeys.contains(name))
                undocumented.add(name);
        }

        assertTrue("Config fields missing from both cassandra.yaml and cassandra_latest.yaml. " +
                   "Either add them to a yaml file or annotate with @Hidden in Config.java:\n" +
                   undocumented,
                   undocumented.isEmpty());
    }

    private static Set<String> extractYamlKeys() throws IOException
    {
        Set<String> keys = new HashSet<>();
        extractKeysFromFile(CONF_DIR.resolve("cassandra.yaml"), keys);
        extractKeysFromFile(CONF_DIR.resolve("cassandra_latest.yaml"), keys);
        return keys;
    }

    private static void extractKeysFromFile(Path path, Set<String> keys) throws IOException
    {
        if (!Files.exists(path))
            return;

        String content = new String(Files.readAllBytes(path));
        Matcher matcher = YAML_KEY_PATTERN.matcher(content);
        while (matcher.find())
        {
            keys.add(matcher.group(1));
        }
    }

    private static boolean isExcluded(String fieldName)
    {
        try
        {
            Field field = findField(Config.class, fieldName);
            if (field == null)
                return false;

            return field.isAnnotationPresent(Hidden.class)
                   || field.isAnnotationPresent(Deprecated.class);
        }
        catch (Exception e)
        {
            return false;
        }
    }

    private static Field findField(Class<?> clazz, String snakeCaseName)
    {
        for (Class<?> c = clazz; c != null; c = c.getSuperclass())
        {
            for (Field f : c.getDeclaredFields())
            {
                if (f.getName().equals(snakeCaseName))
                    return f;
            }
        }
        return null;
    }
}
