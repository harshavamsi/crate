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

package io.crate.planner.optimizer.iterative.rule;

import java.util.List;

import io.crate.common.collections.Lists2;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.iterative.GroupReferenceResolver;

public final class Util {

    private Util() {
    }

    /**
     * @return a new Plan where parent-child (A-B-C) are exchanged to child-parent (B-A-C)
     */
    static LogicalPlan transpose(LogicalPlan parent, LogicalPlan child, GroupReferenceResolver resolver) {
        return child.replaceSources(List.of(
            parent.replaceSources(Lists2.map(child.sources(), resolver::apply)
            )));
    }
}
