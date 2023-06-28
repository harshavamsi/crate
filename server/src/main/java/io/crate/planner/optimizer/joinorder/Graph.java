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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import io.crate.analyze.relations.QuerySplitter;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.format.Style;
import io.crate.planner.operators.Collect;
import io.crate.planner.operators.HashJoin;
import io.crate.planner.operators.JoinPlan;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.LogicalPlanVisitor;
import io.crate.planner.operators.NestedLoopJoin;
import io.crate.planner.optimizer.iterative.GroupReference;

public class Graph {

    private final LogicalPlan root;
    private final List<LogicalPlan> nodes;
    private final Map<Integer, Set<HyperEdge>> edges;

    public Graph(LogicalPlan root, List<LogicalPlan> nodes, Map<Integer, Set<HyperEdge>> edges) {
        this.root = root;
        this.nodes = nodes;
        this.edges = edges;
    }

    public Graph joinWith(LogicalPlan root, Graph other, Map<Integer, Set<HyperEdge>> moreEdges) {
        for (LogicalPlan node : other.nodes) {
            assert !edges.containsKey(node) : "Nodes can not be in both graphs";
        }

        var newNodes = new ArrayList<LogicalPlan>();
        newNodes.addAll(nodes);
        newNodes.addAll(other.nodes);

        var newEdges = new HashMap<Integer, Set<HyperEdge>>();
        newEdges.putAll(edges);
        newEdges.putAll(other.edges);
        newEdges.putAll(moreEdges);


        return new Graph(root, newNodes, newEdges);
    }

    public List<LogicalPlan> nodes() {
        return nodes;
    }

    public Map<Integer, Set<HyperEdge>> edges() {
        return edges;
    }

    public LogicalPlan root() {
        return root;
    }

    public static class HyperEdge {
        public final Set<Integer> from;
        public final Set<Integer> to;

        public HyperEdge(Set<Integer> left, Set<Integer> right) {
            this.from = left;
            this.to = right;
        }

        boolean isSimple() {
            return from.size() == 1 && to.size() == 1;
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
            return super.visitPlan(logicalPlan, context);
        }

        @Override
        public Graph visitGroupReference(GroupReference groupReference, Map<Symbol, LogicalPlan> context) {
            var resolved = resolvePlan.apply(groupReference);
            return resolved.accept(this, context);
        }

        @Override
        public Graph visitNestedLoopJoin(NestedLoopJoin joinPlan, Map<Symbol, LogicalPlan> context) {
            return visitJoin(joinPlan, context);
        }

        @Override
        public Graph visitHashJoin(HashJoin joinPlan, Map<Symbol, LogicalPlan> context) {
            return visitJoin(joinPlan, context);
        }

        public Graph visitJoin(JoinPlan joinPlan, Map<Symbol, LogicalPlan> context) {
            Graph left = joinPlan.lhs().accept(this, context);
            Graph right = joinPlan.rhs().accept(this, context);

            var joinCondition = joinPlan.joinCondition();
            assert joinCondition != null : "Join condition cannot be null to build graph";

            // find equi-join conditions such as `a.x = b.y` and create edges
            var split = QuerySplitter.split(joinCondition);
            var edges = new HashMap<Integer, Set<HyperEdge>>();
            for (var entry : split.entrySet()) {
                if (entry.getKey().size() == 2) {
                    if (entry.getValue() instanceof io.crate.expression.symbol.Function f) {
                        var a = f.arguments().get(0);
                        var b = f.arguments().get(1);
                        var from = context.get(a);
                        var to = context.get(b);
                        assert from != null & to != null :
                            "Invalid join condition to build a graph " + joinCondition.toString(Style.QUALIFIED);
                        edges.put(context.get(a).id(), Set.of(new HyperEdge(Set.of(from.id()), Set.of(to.id()))));
                    }
                }
            }
            return left.joinWith(joinPlan, right, edges);
        }

        @Override
        public Graph visitCollect(Collect collect, Map<Symbol, LogicalPlan> context) {
            for (Symbol output : collect.outputs()) {
                context.put(output, collect);
            }
            return new Graph(collect, List.of(collect), Map.of());
        }
    }

}
