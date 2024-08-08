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

package io.crate.copy.azure;

import static io.crate.copy.azure.AzureFileOutput.azureResourcePath;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.opendal.Entry;
import org.apache.opendal.Operator;
import org.elasticsearch.common.settings.Settings;
import org.jetbrains.annotations.Nullable;

import io.crate.execution.engine.collect.files.FileInput;
import io.crate.execution.engine.collect.files.Globs.GlobPredicate;
import io.crate.execution.engine.collect.files.opendal.Scheme;

/**
 * File reading components operate with URI.
 * All URI-s in the public API follow the contract "outgoing/incoming" URI is Azure compatible.
 * This is accomplished by transforming user provided URI to the Azure compatible format only once.
 * Outgoing URI-s are then used by other components and sent back to this component,
 * so outgoing format (expandURI) implicitly dictates incoming URI-s format (getStream).
 */
public class AzureFileInput implements FileInput {

    private static final Pattern HAS_GLOBS_PATTERN = Pattern.compile("^((azblob://)[^\\*]*/)[^\\*]*\\*.*");

    private final Map<String, String> config;
    private final URI uri;
    private Operator operator;
    private final GlobPredicate uriPredicate;
    private final String preGlobPath;

    /**
     * @param uri is in user provided format (azblob://path/to/dir)
     */
    public AzureFileInput(URI uri, Settings settings) {
        config = new HashMap<>();
        for (String property: Scheme.AZBLOB.properties()) {
            var value = settings.get(property);
            if (value != null) {
                config.put(property, settings.get(property));
            }
        }
        this.uri = URI.create(azureResourcePath(uri, config.get("account_name")));
        // Pre-glob path operates with user-provided URI as GLOB pattern reflects user facing COPY FROM uri format.
        this.preGlobPath = toPreGlobPath(uri, config.get("account_name"));
        // Predicate is build on top of transformed URI, because OpenDAL list API operates with Azure specific format (contains account_name).
        this.uriPredicate = new GlobPredicate(this.uri.toString());
    }

    /**
     * @return List<URI> in Azure compatible format.
     */
    @Override
    public List<URI> expandUri() throws IOException {
        Operator operator = operator();
        if (isGlobbed() == false) {
            return List.of(uri);
        }
        List<URI> uris = new ArrayList<>();
        List<Entry> entries = operator.list(preGlobPath);
        for (Entry entry: entries) {
            var path = entry.getPath();
            if (uriPredicate.test(path)) {
                uris.add(URI.create(path));
            }
        }
        return uris;
    }

    /**
     * @param uri is in Azure compatible format.
     */
    @Override
    public InputStream getStream(URI uri) throws IOException {
        return operator().createInputStream(uri.toString());
    }

    @Override
    public boolean isGlobbed() {
        return preGlobPath != null;
    }

    @Override
    public URI uri() {
        return uri;
    }

    @Override
    public boolean sharedStorageDefault() {
        return true;
    }

    /**
     * <p>
     *  Creates an operator if not yet created.
     * </p>
     *
     *  Operator is closable (via parent NativeObject).
     *  Closing InputStream is taken care of by the caller.
     *  and closing OperatorInputStream closes Operator (closes its NativeHandle).
     */
    private Operator operator() {
        if (operator == null) {
            operator = Operator.of(Scheme.AZBLOB.value(), config);
        }
        return operator;
    }

    /**
     * @return pre-glob path in Azure compatible format.
     */
    @Nullable
    public static String toPreGlobPath(URI uri, String accountName) {
        Matcher hasGlobMatcher = HAS_GLOBS_PATTERN.matcher(uri.toString());
        if (hasGlobMatcher.matches()) {
            return azureResourcePath(URI.create(hasGlobMatcher.group(1)), accountName);
        }
        return null;
    }
}
