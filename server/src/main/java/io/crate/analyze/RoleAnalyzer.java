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

package io.crate.analyze;

import static io.crate.role.Role.Properties.JWT_KEY;
import static io.crate.role.Role.Properties.PASSWORD_KEY;

import java.util.HashMap;
import java.util.Map;

import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.FieldProvider;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.settings.session.SessionSettingRegistry;
import io.crate.sql.tree.AlterRoleReset;
import io.crate.sql.tree.AlterRoleSet;
import io.crate.sql.tree.CreateRole;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.GenericProperties;

public class RoleAnalyzer {

    private final NodeContext nodeCtx;
    private final SessionSettingRegistry sessionSettingRegistry;

    RoleAnalyzer(NodeContext nodeCtx, SessionSettingRegistry sessionSettingRegistry) {
        this.nodeCtx = nodeCtx;
        this.sessionSettingRegistry = sessionSettingRegistry;
    }

    public AnalyzedCreateRole analyze(CreateRole node,
                                      ParamTypeHints paramTypeHints,
                                      CoordinatorTxnCtx txnContext) {
        GenericProperties<Symbol> properties = validateProperties(node.properties(), paramTypeHints, txnContext);
        return new AnalyzedCreateRole(node.name(), node.isUser(), properties);
    }

    public AnalyzedAlterRole analyze(AlterRoleSet<Expression> node,
                                     ParamTypeHints paramTypeHints,
                                     CoordinatorTxnCtx txnContext) {
        GenericProperties<Symbol> properties = validateProperties(node.properties(), paramTypeHints, txnContext);
        return new AnalyzedAlterRole(node.name(), properties, false);
    }

    public AnalyzedAlterRole analyze(AlterRoleReset node) {
        if (node.property() == null) {
            return new AnalyzedAlterRole(node.name(), new GenericProperties<>(Map.of()), true);
        }
        validateProperty(node.property());
        Map<String, Symbol> properties = new HashMap<>();
        properties.put(node.property(), Literal.NULL);
        return new AnalyzedAlterRole(node.name(), new GenericProperties<>(properties), true);
    }

    private GenericProperties<Symbol> validateProperties(GenericProperties<Expression> properties,
                                                         ParamTypeHints paramTypeHints,
                                                         CoordinatorTxnCtx txnContext) {
        GenericProperties<Symbol> validatedProperties = mappedProperties(properties, paramTypeHints, txnContext);
        for (String property : properties.keys().stream().toList()) {
            validateProperty(property);
        }
        return validatedProperties;
    }

    private void validateProperty(String property) {
        if (!PASSWORD_KEY.equals(property) && !JWT_KEY.equals(property)
            && sessionSettingRegistry.settings().get(property) == null) {

            throw new IllegalArgumentException("Invalid session setting: '" + property + "'");
        }
    }

    private GenericProperties<Symbol> mappedProperties(GenericProperties<Expression> properties,
                                                       ParamTypeHints paramTypeHints,
                                                       CoordinatorTxnCtx txnContext) {
        ExpressionAnalysisContext exprContext = new ExpressionAnalysisContext(txnContext.sessionSettings());
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
            txnContext,
            nodeCtx,
            paramTypeHints,
            FieldProvider.UNSUPPORTED,
            null
        );
        return properties.map(x -> expressionAnalyzer.convert(x, exprContext));
    }
}
