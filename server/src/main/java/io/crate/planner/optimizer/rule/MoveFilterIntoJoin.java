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

package io.crate.planner.optimizer.rule;

import static io.crate.planner.optimizer.matcher.Pattern.typeOf;
import static io.crate.planner.optimizer.matcher.Patterns.source;
import static io.crate.planner.optimizer.rule.ExtractConstantJoinCondition.numberOfRelationsUsed;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import io.crate.analyze.relations.QuerySplitter;
import io.crate.expression.operator.AndOperator;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.NodeContext;
import io.crate.metadata.RelationName;
import io.crate.metadata.TransactionContext;
import io.crate.planner.operators.Filter;
import io.crate.planner.operators.JoinPlan;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.costs.PlanStats;
import io.crate.planner.optimizer.matcher.Capture;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;
import io.crate.sql.tree.JoinType;

public class MoveFilterIntoJoin implements Rule<Filter> {

    private final Capture<JoinPlan> joinCapture;
    private final Pattern<Filter> pattern;

    public MoveFilterIntoJoin() {
        this.joinCapture = new Capture<>();
        this.pattern = typeOf(Filter.class)
            .with(source(),
                typeOf(JoinPlan.class)
                    .capturedAs(joinCapture)
            );
    }

    @Override
    public Pattern<Filter> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(Filter filter,
                             Captures captures,
                             PlanStats planStats,
                             TransactionContext txnCtx,
                             NodeContext nodeCtx,
                             UnaryOperator<LogicalPlan> resolvePlan) {
        var join = captures.get(joinCapture);
        var query = filter.query();
        var allConditions = QuerySplitter.split(query);
        var constantConditions = new HashMap<Set<RelationName>, Symbol>(allConditions.size());
        var nonConstantConditions = new HashMap<Set<RelationName>, Symbol>(allConditions.size());
        for (var condition : allConditions.entrySet()) {
            if (numberOfRelationsUsed(condition.getValue()) <= 1) {
                constantConditions.put(condition.getKey(), condition.getValue());
            } else {
                nonConstantConditions.put(condition.getKey(), condition.getValue());
            }
        }
        if (nonConstantConditions.isEmpty()) {
            return null;
        }
        var result = new ArrayList<Symbol>();
        for (var entries : nonConstantConditions.entrySet()) {
            if (new HashSet<>(join.getRelationNames()).containsAll(entries.getKey())) {
                result.add(entries.getValue());
            }
        }
        if (result.isEmpty()) {
            return null;
        } else {
            JoinPlan newJoin = null;
            if (join.joinType() == JoinType.CROSS) {
                newJoin = new JoinPlan(join.lhs(), join.rhs(), JoinType.INNER, AndOperator.join(result));
            } else {
                result.add(join.joinCondition());
                newJoin = new JoinPlan(join.lhs(), join.rhs(), join.joinType(), AndOperator.join(result));
            }
            return Filter.create(newJoin, AndOperator.join(constantConditions.values()));
        }
    }
}
