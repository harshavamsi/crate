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

package io.crate.execution.engine.aggregation.impl;


import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.datasketches.frequencies.ErrorType;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.datasketches.memory.Memory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.jetbrains.annotations.Nullable;

import io.crate.Streamer;
import io.crate.data.Input;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.execution.engine.aggregation.DocValueAggregator;
import io.crate.execution.engine.aggregation.impl.templates.BinaryDocValueAggregator;
import io.crate.execution.engine.aggregation.impl.templates.SortedNumericDocValueAggregator;
import io.crate.expression.reference.doc.lucene.LuceneReferenceResolver;
import io.crate.expression.symbol.Literal;
import io.crate.memory.MemoryManager;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.Scalar;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.statistics.SketchStreamer;
import io.crate.types.ArrayType;
import io.crate.types.ByteType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.DoubleType;
import io.crate.types.FloatType;
import io.crate.types.GeoPointType;
import io.crate.types.IntegerType;
import io.crate.types.IpType;
import io.crate.types.LongType;
import io.crate.types.ShortType;
import io.crate.types.StringType;
import io.crate.types.TimestampType;
import io.crate.types.TypeSignature;

public class TopKAggregation extends AggregationFunction<TopKAggregation.TopKState, List<Object>> {

    public static final String NAME = "top_k";

    static final Signature DEFAULT_SIGNATURE =
        Signature.builder(NAME, FunctionType.AGGREGATE)
            .argumentTypes(TypeSignature.parse("V"))
            .returnType(new ArrayType<>(DataTypes.UNTYPED_OBJECT).getTypeSignature())
            .features(Scalar.Feature.DETERMINISTIC)
            .typeVariableConstraints(typeVariable("V"))
            .build();

    static final Signature PARAMETER_SIGNATURE =
        Signature.builder(NAME, FunctionType.AGGREGATE)
            .argumentTypes(TypeSignature.parse("V"),
                DataTypes.INTEGER.getTypeSignature())
            .returnType(new ArrayType<>(DataTypes.UNTYPED_OBJECT).getTypeSignature())
            .features(Scalar.Feature.DETERMINISTIC)
            .typeVariableConstraints(typeVariable("V"))
            .build();

    static {
        DataTypes.register(TopKAggregation.TopKStateType.ID, in -> TopKAggregation.TopKStateType.INSTANCE);
    }

    public static void register(Functions.Builder builder) {
        builder.add(
            DEFAULT_SIGNATURE,
            TopKAggregation::new
        );

        builder.add(
            PARAMETER_SIGNATURE,
            TopKAggregation::new
        );
    }

    private final Signature signature;
    private final BoundSignature boundSignature;

    private TopKAggregation(Signature signature, BoundSignature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
    }

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public BoundSignature boundSignature() {
        return boundSignature;
    }


    @Nullable
    @Override
    public TopKState newState(RamAccounting ramAccounting,
                              Version indexVersionCreated,
                              Version minNodeInCluster,
                              MemoryManager memoryManager) {
        return new TopKState(new ItemsSketch<>(32), 8);
    }

    @Override
    public TopKState iterate(RamAccounting ramAccounting,
                             MemoryManager memoryManager,
                             TopKState state,
                             Input<?>... args) throws CircuitBreakingException {
        if (state.itemsSketch.isEmpty() && args.length == 2) {
            Integer limit = (Integer) args[1].value();
            if (limit <= 0) {
                throw new IllegalArgumentException("Invalid value for parameter k for top-k aggregation");
            }
            state = new TopKState(new ItemsSketch<>(maxMapSize(limit)), limit);
        }
        Object value = args[0].value();
        state.itemsSketch.update(value);
        return state;
    }

    private int maxMapSize(int x) {
        // max map size should be 4 * the limit based on the power of 2 to avoid errors
        return (int) Math.pow(2, Math.ceil(Math.log(x) / Math.log(2))) * 4;
    }

    @Override
    public TopKState reduce(RamAccounting ramAccounting, TopKState state1, TopKState state2) {
        return new TopKState(state1.itemsSketch.merge(state2.itemsSketch), Math.min(state1.limit, state2.limit));
    }

    @Override
    public List<Object> terminatePartial(RamAccounting ramAccounting, TopKState state) {
        ItemsSketch.Row<Object>[] frequentItems = state.itemsSketch.getFrequentItems(ErrorType.NO_FALSE_NEGATIVES);
        int limit = Math.min(frequentItems.length, state.limit);
        var result = new ArrayList<>(limit);
        for (int i = 0; i < limit; i++) {
            var item = frequentItems[i];
            var row = new LinkedHashMap<>(2);
            row.put("item", item.getItem());
            row.put("frequency", item.getEstimate());
            result.add(row);
        }
        return result;
    }

    public DataType<?> partialType() {
        return TopKStateType.INSTANCE;
    }

    public record TopKState(ItemsSketch<Object> itemsSketch, int limit) {
    }

    static final class TopKStateType extends DataType<TopKState> implements Streamer<TopKState> {

        public static final int ID = 4232;
        public static final TopKStateType INSTANCE = new TopKStateType();

        @Override
        public int id() {
            return ID;
        }

        @Override
        public Precedence precedence() {
            return Precedence.CUSTOM;
        }

        @Override
        public String getName() {
            return "top_k_state";
        }

        @Override
        public Streamer<TopKState> streamer() {
            return this;
        }

        @Override
        public TopKState sanitizeValue(Object value) {
            return (TopKState) value;
        }

        @Override
        public TopKState readValueFrom(StreamInput in) throws IOException {
            int limit = in.readInt();
            SketchStreamer<Object> streamer = new SketchStreamer<>(DataTypes.UNDEFINED);
            return new TopKState(ItemsSketch.getInstance(Memory.wrap(in.readByteArray()), streamer), limit);
        }

        @Override
        public void writeValueTo(StreamOutput out, TopKState state) throws IOException {
            out.writeInt(state.limit);
            SketchStreamer<Object> streamer = new SketchStreamer<>(DataTypes.UNDEFINED);
            out.writeByteArray(state.itemsSketch.toByteArray(streamer));
        }

        @Override
        public long valueBytes(TopKState value) {
            throw new UnsupportedOperationException("valueSize is not implemented for TopKStateType");
        }

        @Override
        public int compare(TopKState s1, TopKState s2) {
            return 0;
        }
    }

    private DocValueAggregator<?> getDocValueAggregator(Reference ref) {
        if (!ref.hasDocValues()) {
            return null;
        }
        switch (ref.valueType().id()) {
            case ByteType.ID:
            case ShortType.ID:
            case IntegerType.ID:
            case LongType.ID:
            case TimestampType.ID_WITH_TZ:
            case TimestampType.ID_WITHOUT_TZ:
            case FloatType.ID:
            case DoubleType.ID:
            case GeoPointType.ID:
                return new SortedNumericDocValueAggregator<>(
                    ref.storageIdent(),
                    (ramAccounting, memoryManager, minNodeVersion) -> {
                        return new TopKState(new ItemsSketch<>(32), 8);
                    },
                    (values, state) -> {
                        state.itemsSketch.update(values.nextValue());
                    }
                );
            case IpType.ID:
            case StringType.ID:
                return new BinaryDocValueAggregator<>(
                    ref.storageIdent(),
                    (ramAccounting, memoryManager, minNodeVersion) -> {
                        return new TopKState(new ItemsSketch<>(32), 8);
                    },
                    (values, state) -> {
                        long ord = values.nextOrd();
                        BytesRef value = values.lookupOrd(ord);
                        state.itemsSketch.update(value.utf8ToString());
                    }
                );
            default:
                return null;
        }
    }

    @Nullable
    @Override
    public DocValueAggregator<?> getDocValueAggregator(LuceneReferenceResolver referenceResolver,
                                                       List<Reference> aggregationReferences,
                                                       DocTableInfo table,
                                                       List<Literal<?>> optionalParams) {
        if (aggregationReferences.size() != 1) {
            return null;
        }
        Reference reference = aggregationReferences.get(0);
        if (!reference.hasDocValues()) {
            return null;
        }
        return getDocValueAggregator(reference);
    }

}
