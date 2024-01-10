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

package io.crate.testing;

import org.assertj.core.api.AbstractAssert;

import io.crate.role.PrivilegeType;

public final class PrivilegeResolutionAssert extends AbstractAssert<PrivilegeResolutionAssert, PrivilegeType> {

    public PrivilegeResolutionAssert(PrivilegeType actual) {
        super(actual, PrivilegeResolutionAssert.class);
    }

    public PrivilegeResolutionAssert isGranted() {
        isNotNull();
        if (actual != PrivilegeType.GRANT) {
            failWithMessage("Expected PrivilegeState to be GRANTED");
        }
        return this;
    }

    public PrivilegeResolutionAssert isDenied() {
        isNotNull();
        if (actual != PrivilegeType.DENY) {
            failWithMessage("Expected PrivilegeState to be DENIED");
        }
        return this;
    }

    public PrivilegeResolutionAssert isMissing() {
        isNotNull();
        if (actual != PrivilegeType.REVOKE) {
            failWithMessage("Expected PrivilegeState to be missing");
        }
        return this;
    }
}
