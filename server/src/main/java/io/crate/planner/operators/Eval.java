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

package io.crate.planner.operators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.jetbrains.annotations.Nullable;

import io.crate.analyze.OrderBy;
import io.crate.common.collections.Lists2;
import io.crate.data.Row;
import io.crate.execution.dsl.projection.EvalProjection;
import io.crate.execution.dsl.projection.builder.InputColumns;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.expression.symbol.FetchMarker;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitors;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.Merge;
import io.crate.planner.PlannerContext;
import io.crate.planner.PositionalOrderBy;


/**
 * The Eval operator is producing the values for all selected expressions.
 *
 * <p>
 * This can be a simple re-arranging of expressions or the evaluation of scalar functions
 * </p>
 */
public final class Eval extends ForwardingLogicalPlan {

    private final List<Symbol> outputs;
    private final int id;

    public static LogicalPlan create(int id, LogicalPlan source, List<Symbol> outputs) {
        if (source.outputs().equals(outputs)) {
            return source;
        }
        return new Eval(id, source, outputs);
    }

    Eval(int id, LogicalPlan source, List<Symbol> outputs) {
        super(id, source);
        this.outputs = outputs;
        this.id = id;
    }

    @Override
    public ExecutionPlan build(DependencyCarrier executor,
                               PlannerContext plannerContext,
                               Set<PlanHint> planHints,
                               ProjectionBuilder projectionBuilder,
                               int limit,
                               int offset,
                               @Nullable OrderBy order,
                               @Nullable Integer pageSizeHint,
                               Row params,
                               SubQueryResults subQueryResults) {
        ExecutionPlan executionPlan = source.build(
            executor, plannerContext, planHints, projectionBuilder, limit, offset, null, pageSizeHint, params, subQueryResults);
        if (outputs.equals(source.outputs())) {
            return executionPlan;
        }
        return addEvalProjection(plannerContext, executionPlan, params, subQueryResults);
    }

    @Override
    public List<Symbol> outputs() {
        return outputs;
    }

    @Override
    public LogicalPlan replaceSources(List<LogicalPlan> sources) {
        return new Eval(id, Lists2.getOnlyElement(sources), outputs);
    }

    @Override
    public LogicalPlan pruneOutputsExcept(Collection<Symbol> outputsToKeep) {
        LogicalPlan newSource = source.pruneOutputsExcept(outputsToKeep);
        if (source == newSource) {
            return this;
        }
        return new Eval(id, newSource, List.copyOf(outputsToKeep));
    }

    @Nullable
    @Override
    public FetchRewrite rewriteToFetch(Collection<Symbol> usedColumns) {
        FetchRewrite fetchRewrite = source.rewriteToFetch(usedColumns);
        if (fetchRewrite == null) {
            return null;
        }
        LogicalPlan newSource = fetchRewrite.newPlan();
        Function<Symbol, Symbol> mapToFetchStubs = fetchRewrite.mapToFetchStubs();
        LinkedHashMap<Symbol, Symbol> newReplacedOutputs = new LinkedHashMap<>();
        ArrayList<Symbol> newOutputs = new ArrayList<>();
        for (Symbol sourceOutput : newSource.outputs()) {
            if (sourceOutput instanceof FetchMarker) {
                newOutputs.add(sourceOutput);
            }
        }
        for (Symbol output : outputs) {
            newReplacedOutputs.put(output, mapToFetchStubs.apply(output));
            if (SymbolVisitors.any(newSource.outputs()::contains, output)) {
                newOutputs.add(output);
            }
        }
        return new FetchRewrite(newReplacedOutputs, Eval.create(id, newSource, newOutputs));
    }

    private ExecutionPlan addEvalProjection(PlannerContext plannerContext,
                                            ExecutionPlan executionPlan,
                                            Row params,
                                            SubQueryResults subQueryResults) {
        PositionalOrderBy orderBy = executionPlan.resultDescription().orderBy();
        PositionalOrderBy newOrderBy = null;
        SubQueryAndParamBinder binder = new SubQueryAndParamBinder(params, subQueryResults);
        List<Symbol> boundOutputs = Lists2.map(outputs, binder);
        if (orderBy != null) {
            newOrderBy = orderBy.tryMapToNewOutputs(source.outputs(), boundOutputs);
            if (newOrderBy == null) {
                executionPlan = Merge.ensureOnHandler(executionPlan, plannerContext);
            }
        }
        InputColumns.SourceSymbols ctx = new InputColumns.SourceSymbols(Lists2.map(source.outputs(), binder));
        EvalProjection projection = new EvalProjection(InputColumns.create(boundOutputs, ctx));
        executionPlan.addProjection(
            projection,
            executionPlan.resultDescription().limit(),
            executionPlan.resultDescription().offset(),
            newOrderBy
        );
        return executionPlan;
    }

    @Override
    public int id() {
        return id;
    }

    @Override
    public String toString() {
        return "Eval{" +
               "src=" + source +
               ", out=" + outputs +
               '}';
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitEval(this, context);
    }
}
