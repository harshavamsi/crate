/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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


import static io.crate.planner.optimizer.iterative.matcher.Pattern.typeOf;
import static io.crate.planner.optimizer.iterative.matcher.Patterns.source;
import static io.crate.planner.optimizer.iterative.rule.Util.transpose;

import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.planner.operators.Eval;
import io.crate.planner.operators.Limit;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.iterative.GroupReferenceResolver;
import io.crate.planner.optimizer.iterative.Rule;
import io.crate.planner.optimizer.iterative.matcher.Capture;
import io.crate.planner.optimizer.iterative.matcher.Captures;
import io.crate.planner.optimizer.iterative.matcher.Pattern;
import io.crate.statistics.TableStats;

public class MoveLimitBeneathEval implements Rule<Limit> {

    private final Capture<Eval> evalCapture;
    private final Pattern<Limit> pattern;

    public MoveLimitBeneathEval() {
        this.evalCapture = new Capture<>();
        this.pattern = typeOf(Limit.class)
            .with(source(), typeOf(Eval.class).capturedAs(evalCapture));
    }

    @Override
    public Pattern<Limit> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(Limit limit,
                             Captures captures,
                             TableStats tableStats,
                             TransactionContext txnCtx,
                             NodeContext nodeCtx,
                             GroupReferenceResolver resolver) {
        Eval eval = captures.get(evalCapture);
        return transpose(limit, eval, resolver);
    }
}
