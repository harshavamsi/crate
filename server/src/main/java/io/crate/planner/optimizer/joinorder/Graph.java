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

package io.crate.planner.optimizer.joinorder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.jetbrains.annotations.Nullable;

import io.crate.analyze.relations.QuerySplitter;
import io.crate.common.collections.Sets;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.format.Style;
import io.crate.planner.operators.AbstractJoinPlan;
import io.crate.planner.operators.HashJoin;
import io.crate.planner.operators.JoinPlan;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.LogicalPlanVisitor;
import io.crate.planner.operators.NestedLoopJoin;
import io.crate.planner.optimizer.iterative.GroupReference;
import io.crate.sql.tree.JoinType;

public class Graph {

    private final LogicalPlan root;
    private final List<LogicalPlan> nodes;
    private final Map<Integer, LogicalPlan> nodesById;
    private final Map<Integer, Set<Edge>> edges;

    public Graph(LogicalPlan root, List<LogicalPlan> nodes, Map<Integer, Set<Edge>> edges) {
        this.root = root;
        this.edges = edges;
        this.nodes = nodes;
        this.nodesById = new HashMap<>();
        for (LogicalPlan node : nodes) {
            this.nodesById.put(node.id(), node);
        }
    }

    public Graph joinWith(LogicalPlan root, Graph other, Map<Integer, Set<Edge>> moreEdges) {
        for (LogicalPlan node : other.nodes) {
            assert !edges.containsKey(node) : "Nodes can not be in both graphs";
        }

        var newNodes = new ArrayList<LogicalPlan>();
        newNodes.addAll(nodes);
        newNodes.addAll(other.nodes);

        var newEdges = mergeMap(edges, other.edges);
        newEdges = mergeMap(newEdges, moreEdges);

        return new Graph(root, newNodes, newEdges);
    }

    private static Map<Integer, Set<Edge>> mergeMap(Map<Integer, Set<Edge>> a,  Map<Integer, Set<Edge>> b) {
        var copyOfB = new HashMap<>(b);
        var result = new HashMap<Integer, Set<Edge>>();
        for (var key : a.keySet()) {
            Set<Edge> edges1 = a.get(key);
            Set<Edge> edges2 = copyOfB.remove(key);
            if (edges2 != null) {
                result.put(key, Sets.union(edges1, edges2));
            } else {
                result.put(key, edges1);
            }
        }
        result.putAll(copyOfB);
        return result;
    }

    public List<LogicalPlan> nodes() {
        return nodes;
    }

    public Map<Integer, Set<Edge>> edges() {
        return edges;
    }

    public LogicalPlan root() {
        return root;
    }

    public int size() {
        return nodes.size();
    }

    @Nullable
    public LogicalPlan nodeByPosition(int i) {
        return nodes.get(i);
    }

    @Nullable
    public LogicalPlan nodeById(int i) {
        return nodesById.get(i);
    }

    public Set<Edge> getEdges(LogicalPlan node) {
        var result = edges.get(node.id());
        if (result == null) {
            return Set.of();
        }
        return result;
    }

    public record Edge(
        LogicalPlan from,
        @Nullable Symbol fromVariable,
        LogicalPlan to,
        @Nullable Symbol toVariable,
        JoinType joinType,
        Symbol joinCondition) {

        public boolean isCrossJoin() {
            return fromVariable == null && toVariable == null;
        }

        public Set<Integer> ids() {
            var result = new HashSet<Integer>(2);
            if (from != null) {
                result.add(from.id());
            }
            if (to != null) {
                result.add(to().id());
            }
            return result;
        }

        @Override
        public String toString() {
            return "Edge{" +
                   "from=" + fromVariable.toString(Style.QUALIFIED) +
                   ", to=" + toVariable.toString(Style.QUALIFIED) +
                   '}';
        }
    }

    public static Graph create(LogicalPlan plan, Function<LogicalPlan, LogicalPlan> resolvePlan) {
        var visitor = new Visitor(resolvePlan);
        var context = new HashMap<Symbol, LogicalPlan>();
        return plan.accept(visitor, context);
    }

    private static class Visitor extends LogicalPlanVisitor<Map<Symbol, LogicalPlan>, Graph> {

        private final Function<LogicalPlan, LogicalPlan> resolvePlan;

        public Visitor(Function<LogicalPlan, LogicalPlan> resolvePlan) {
            this.resolvePlan = resolvePlan;
        }

        @Override
        public Graph visitPlan(LogicalPlan logicalPlan, Map<Symbol, LogicalPlan> context) {
            for (Symbol output : logicalPlan.outputs()) {
                context.put(output, logicalPlan);
            }
            return new Graph(logicalPlan, List.of(logicalPlan), Map.of());
        }

        @Override
        public Graph visitGroupReference(GroupReference groupReference, Map<Symbol, LogicalPlan> context) {
            return resolvePlan.apply(groupReference).accept(this, context);
        }

        //TODO Handle filters

        @Override
        public Graph visitJoinPlan(JoinPlan logicalPlan, Map<Symbol, LogicalPlan> context) {
            return visitJoin(logicalPlan, context);
        }

        @Override
        public Graph visitHashJoin(HashJoin logicalPlan, Map<Symbol, LogicalPlan> context) {
            return visitJoin(logicalPlan, context);
        }

        @Override
        public Graph visitNestedLoopJoin(NestedLoopJoin logicalPlan, Map<Symbol, LogicalPlan> context) {
            return visitJoin(logicalPlan, context);
        }

        public Graph visitJoin(AbstractJoinPlan joinPlan, Map<Symbol, LogicalPlan> context) {
            if (joinPlan.joinType().isOuter()) {
                return visitPlan(joinPlan, context);
            }
            var left = joinPlan.lhs().accept(this, context);
            var right = joinPlan.rhs().accept(this, context);

            if (joinPlan.joinType() == JoinType.CROSS) {
                return left.joinWith(joinPlan, right, Map.of());
            }
            var joinCondition = joinPlan.joinCondition();
            var edges = new HashMap<Integer, Set<Edge>>();
            // if join condition is null, we have a cross-join
            if (joinCondition != null) {
                // find equi-join conditions such as `a.x = b.y` and create edges
                // TODO deal with the rest of the filters such as `a.x >= 1`
                var split = QuerySplitter.split(joinCondition);
                for (var entry : split.entrySet()) {
                    if (entry.getKey().size() == 2) {
                        if (entry.getValue() instanceof io.crate.expression.symbol.Function f) {
                            if (f.name().equals(EqOperator.NAME)) {
                                var fromSymbol = f.arguments().get(0);
                                var toSymbol = f.arguments().get(1);
                                var from = context.get(fromSymbol);
                                var to = context.get(toSymbol);
                                if (from == null || to == null) {
                                    continue;
                                }
                                var edge = new Edge(from,
                                                    fromSymbol,
                                                    to,
                                                    toSymbol,
                                                    joinPlan.joinType(),
                                                    joinCondition);
                                insertEdge(edges, edge);
                            }
                        }
                    }
                }
            }
            return left.joinWith(joinPlan, right, edges);
        }

        private static void insertEdge(Map<Integer, Set<Edge>> edges, Edge edge) {
            for (var id : edge.ids()) {
                var result = edges.get(id);
                if (result == null) {
                    result = Set.of(edge);
                } else {
                    result = new HashSet<>(result);
                    result.add(edge);
                }
                edges.put(id, result);
            }
        }
    }

}
