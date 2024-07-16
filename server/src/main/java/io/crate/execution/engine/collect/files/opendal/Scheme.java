/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.execution.engine.collect.files.opendal;

import java.util.List;

/**
 * Supported schemes from https://docs.rs/opendal/latest/opendal/enum.Scheme.html
 * and their configuration properties.
 */
public enum Scheme {

    // See https://docs.rs/opendal/latest/opendal/services/struct.Azblob.html
    AZBLOB("azblob", List.of("root", "container", "endpoint", "account_name", "account_key"));

    private final String value;
    private final List<String> properties;

    Scheme(String value, List<String> properties) {
        this.value = value;
        this.properties = properties;
    }

    public String value() {
        return value;
    }

    public List<String> properties() {
        return properties;
    }
}
