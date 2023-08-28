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

package io.crate.planner.optimizer.rule;

import static io.crate.planner.optimizer.matcher.Pattern.typeOf;
import static java.util.Comparator.comparing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.Function;
import java.util.function.IntSupplier;

import io.crate.expression.operator.AndOperator;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.planner.operators.Eval;
import io.crate.planner.operators.JoinPlan;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.costs.PlanStats;
import io.crate.planner.optimizer.joinorder.Graph;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;
import io.crate.sql.tree.JoinType;

public class ReorderJoinPlan implements Rule<JoinPlan> {

    private final Pattern<JoinPlan> pattern = typeOf(JoinPlan.class).with(j -> j.isReordered() == false);

    @Override
    public Pattern<JoinPlan> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(JoinPlan plan,
                             Captures captures,
                             PlanStats planStats,
                             TransactionContext txnCtx,
                             NodeContext nodeCtx,
                             IntSupplier ids,
                             Function<LogicalPlan, LogicalPlan> resolvePlan) {
        var joinGraph = Graph.create(plan, resolvePlan);
        if (joinGraph.size() < 3) {
            new JoinPlan(
                plan.id(),
                plan.lhs(),
                plan.rhs(),
                plan.joinType(),
                plan.joinCondition(),
                plan.isFiltered(),
                plan.leftRelation(),
                true
            );
        }
        var joinOrder = eliminateCrossJoins(joinGraph);
        if (isOriginalOrder(joinOrder)) {
            new JoinPlan(
                plan.id(),
                plan.lhs(),
                plan.rhs(),
                plan.joinType(),
                plan.joinCondition(),
                plan.isFiltered(),
                plan.leftRelation(),
                true
            );
        }
        var result = buildJoinPlan(joinGraph, joinOrder, ids);
        return Eval.create(
            ids.getAsInt(),
            result,
            plan.outputs()
        );
    }

    /**
     * Basic cross-join elimination without a cost model
     */
    public static List<Integer> eliminateCrossJoins(Graph graph) {
        List<Integer> joinOrder = new ArrayList<>();

        Map<Integer, Integer> priorities = new HashMap<>();
        for (int i = 0; i < graph.size(); i++) {
            priorities.put(graph.nodeByPosition(i).id(), i);
        }

        var nodesToVisit = new PriorityQueue<LogicalPlan>(graph.size(), comparing(node -> priorities.get(node.id())));
        var visited = new HashSet<LogicalPlan>();

        nodesToVisit.add(graph.nodeByPosition(0));

        while (!nodesToVisit.isEmpty()) {
            LogicalPlan node = nodesToVisit.poll();
            if (!visited.contains(node)) {
                visited.add(node);
                joinOrder.add(node.id());
                for (Graph.Edge edge : graph.getEdges(node)) {
                    if (edge.to() != null) {
                        nodesToVisit.add(edge.to());
                    }
                }
            }

            if (nodesToVisit.isEmpty() && visited.size() < graph.size()) {
                // disconnected graph, find new starting point
                Optional<LogicalPlan> firstNotVisitedNode = graph.nodes().stream()
                    .filter(graphNode -> !visited.contains(graphNode))
                    .findFirst();
                firstNotVisitedNode.ifPresent(nodesToVisit::add);
            }
        }
        assert visited.size() == graph.size() : "Invalid state, each node needs to be visited";
        return joinOrder;
    }

    public static LogicalPlan buildJoinPlan(Graph graph,
                                            List<Integer> joinOrder,
                                            IntSupplier ids) {
        LogicalPlan result = graph.nodeById(joinOrder.get(0));
        Set<Integer> alreadyJoinedNodes = new HashSet<>();
        alreadyJoinedNodes.add(result.id());

        for (int i = 1; i < joinOrder.size(); i++) {
            LogicalPlan rightNode = graph.nodeById(joinOrder.get(i));
            alreadyJoinedNodes.add(rightNode.id());

            var criteria = new ArrayList<Symbol>();

            for (var edge : graph.getEdges(rightNode)) {
                // rebuild join conditions
                LogicalPlan targetNode = edge.to();
                if (alreadyJoinedNodes.contains(targetNode.id())) {
                    var fromVariable = edge.fromVariable();
                    var toVariable = edge.toVariable();
                    var condition = EqOperator.of(fromVariable, toVariable);
                    criteria.add(condition);
                }
            }
            JoinType joinType = criteria.isEmpty() ?  JoinType.CROSS : JoinType.INNER;

            result = new JoinPlan(
                ids.getAsInt(),
                result,
                rightNode,
                joinType,
                AndOperator.join(criteria, null),
                false,
                null,
                true
            );
        }
        //TODO handle filters
        return result;
    }

    private static boolean isOriginalOrder(List<Integer> joinOrder) {
        for (int i = 0; i < joinOrder.size(); i++) {
            if (joinOrder.get(i) != i) {
                return false;
            }
        }
        return true;
    }
}
