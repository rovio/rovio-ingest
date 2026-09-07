/*
 * Copyright 2021 Rovio Entertainment Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.rovio.ingest;

import org.junit.jupiter.api.Test;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DruidExpressionParserIT {

    @Test
    public void shouldUseIsolatedDruidAntlrRuntimeInShadedJar() throws Exception {
        Path shadedJar = Path.of(System.getProperty("shadedJar"));
        assertTrue(Files.isRegularFile(shadedJar));

        URL[] classpath = {shadedJar.toUri().toURL()};
        try (URLClassLoader classLoader = new URLClassLoader(classpath, ClassLoader.getPlatformClassLoader())) {
            Class<?> macroTableClass = classLoader.loadClass("org.apache.druid.math.expr.ExprMacroTable");
            Object macroTable = macroTableClass.getMethod("nil").invoke(null);
            Class<?> parserClass = classLoader.loadClass("org.apache.druid.math.expr.Parser");

            Object expression = parserClass
                    .getMethod("parse", String.class, macroTableClass)
                    .invoke(null, "x + 2", macroTable);

            assertNotNull(expression);
            assertEquals("(+ x 2)", expression.toString());

            Class<?> transformClass = classLoader.loadClass(
                    "org.apache.druid.segment.transform.ExpressionTransform");
            Object transform = transformClass
                    .getConstructor(String.class, String.class, macroTableClass)
                    .newInstance("sum", "x + 2", macroTable);
            assertNotNull(transformClass.getMethod("getRowFunction").invoke(transform));

            Class<?> runtimeMetadata = classLoader.loadClass("repackaged.org.antlr.v4.runtime.RuntimeMetaData");
            assertEquals("4.5.3", runtimeMetadata.getField("VERSION").get(null));
            assertThrows(ClassNotFoundException.class,
                    () -> classLoader.loadClass("org.antlr.v4.runtime.RuntimeMetaData"));
        }
    }
}
