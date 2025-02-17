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

package org.apache.ignite.cli.ui;

import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import java.io.IOException;
import javax.inject.Singleton;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

/**
 * Factory for producing JLine {@link Terminal} instances.
 */
@Factory
public class TerminalFactory {
    /**
     * Produce terminal instances.
     *
     * <p>Important: It's always must be a singleton bean. JLine has an issues with building more than 1 terminal instance per process.
     *
     * @return Terminal instance.
     * @throws IOException if an error occurs.
     */
    @Bean(preDestroy = "close")
    @Singleton
    public Terminal terminal() throws IOException {
        return TerminalBuilder.terminal();
    }
}
