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

package io.crate.expression.operator.all;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.function.IntPredicate;
import java.util.stream.StreamSupport;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.lucene.search.Queries;
import org.jetbrains.annotations.Nullable;

import io.crate.expression.operator.AllOperator;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.operator.any.AnyNeqOperator;
import io.crate.expression.predicate.IsNullPredicate;
import io.crate.expression.scalar.ArrayUnnestFunction;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.metadata.Reference;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.sql.tree.ComparisonExpression;

public class AllEqOperator extends AllOperator {

    public static String NAME = OPERATOR_PREFIX + ComparisonExpression.Type.EQUAL.getValue();

    public AllEqOperator(Signature signature, BoundSignature boundSignature, IntPredicate cmp) {
        super(signature, boundSignature, cmp);
    }

    public static Query refMatchesAllArrayLiteral(Reference probe, List<?> nonNullValues, LuceneQueryBuilder.Context context) {
        // col = ALL ([1,2,3]) --> col=1 and col=2 and col=3
        LinkedHashSet<?> uniqueNonNullValues = new LinkedHashSet<>(nonNullValues);
        if (uniqueNonNullValues.size() >= 2) {
            // if `col = 4` then `col = ALL ([1,2,3])` evaluates to `false`
            // if `col = 3` then `col = ALL ([1,2,3])` evaluates to `false`
            return new MatchNoDocsQuery("A single value cannot match more than one unique values");
        }
        String columnName = probe.storageIdent();
        BooleanQuery.Builder andBuilder = new BooleanQuery.Builder();
        for (Object value : nonNullValues) {
            var fromPrimitive = EqOperator.fromPrimitive(
                probe.valueType(),
                columnName,
                value,
                probe.hasDocValues(),
                probe.indexType());
            if (fromPrimitive == null) {
                return null;
            }
            andBuilder.add(fromPrimitive, BooleanClause.Occur.MUST);
        }
        Query exists = IsNullPredicate.refExistsQuery(probe, context, false);
        return new BooleanQuery.Builder()
            .add(andBuilder.build(), BooleanClause.Occur.MUST)
            .add(exists, BooleanClause.Occur.FILTER)
            .build();
    }

    public static Query literalMatchesAllArrayRef(Function function, Literal<?> probe, Reference candidates, LuceneQueryBuilder.Context context) {
        // col = ALL ([1,2,3]) --> col=1 and col=2 and col=3 --> not(col!=1 or col!=2 or col!=3) --> not(col!= any([1,2,3]))
        return Queries.not(AnyNeqOperator.literalMatchesAnyArrayRef(probe, candidates, context));
    }

    @Override
    public @Nullable Query toQuery(Function function, LuceneQueryBuilder.Context context) {
        List<Symbol> args = function.arguments();
        Symbol probe = args.get(0);
        Symbol candidates = args.get(1);
        while (candidates instanceof Function fn && fn.signature().equals(ArrayUnnestFunction.SIGNATURE)) {
            candidates = fn.arguments().get(0);
        }
        if (probe instanceof Literal<?> literal && candidates instanceof Reference ref) {
            return literalMatchesAllArrayRef(function, literal, ref, context);
        } else if (probe instanceof Reference ref && candidates instanceof Literal<?> literal) {
            var nonNullValues = StreamSupport
                .stream(((Iterable<?>) literal.value()).spliterator(), false)
                .filter(Objects::nonNull).toList();
            if (nonNullValues.isEmpty()) {
                return new MatchNoDocsQuery("Cannot match unless there is at least one non-null candidate");
            }
            return refMatchesAllArrayLiteral(ref, nonNullValues, context);
        } else {
            return null;
        }
    }
}
