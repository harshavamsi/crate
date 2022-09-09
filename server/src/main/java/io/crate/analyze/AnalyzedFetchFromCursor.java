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

package io.crate.analyze;

import java.util.List;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import io.crate.expression.symbol.Symbol;

public class AnalyzedFetchFromCursor extends AnalyzedCursor {

    private final int count;

    public static AnalyzedFetchFromCursor safeCreate(int count, AnalyzedStatement analyzedStatement) {
        if (analyzedStatement instanceof AnalyzedCursor analyzedCursor) {
            return new AnalyzedFetchFromCursor(count, analyzedCursor);
        }
        throw new IllegalArgumentException("FetchFromCursor cannot be created");
    }

    private AnalyzedFetchFromCursor(int count, AnalyzedCursor analyzedCursor) {
        super(analyzedCursor.cursorName(), analyzedCursor.query());
        this.count = count;
    }

    public int count() {
        return count;
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> analyzedStatementVisitor, C context) {
        //return this.query().accept(analyzedStatementVisitor, context);
        return analyzedStatementVisitor.visitFetchFromCursor(this, context);
    }

    @Override
    public boolean isWriteOperation() {
        return false;
    }

    @Override
    public void visitSymbols(Consumer<? super Symbol> consumer) {

    }

    @Nullable
    @Override
    public List<Symbol> outputs() {
        return query().outputs();
    }
}
