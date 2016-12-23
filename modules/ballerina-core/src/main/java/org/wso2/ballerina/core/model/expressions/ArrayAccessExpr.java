/*
*  Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
*  Unless required by applicable law or agreed to in writing,
*  software distributed under the License is distributed on an
*  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
*  KIND, either express or implied.  See the License for the
*  specific language governing permissions and limitations
*  under the License.
*/
package org.wso2.ballerina.core.model.expressions;

import org.wso2.ballerina.core.model.NodeVisitor;
import org.wso2.ballerina.core.model.SymbolName;

/**
 * {@code ArrayAccessExpr} represents an array access operation.
 * <p>
 * e.g. x[0] = 5;
 * y = x[0]
 *
 * @since 1.0.0
 */
public class ArrayAccessExpr extends UnaryExpression {

    private SymbolName symbolName;
    private Expression indexExpr;
    private boolean isLHSExpr;


    private ArrayAccessExpr(SymbolName symbolName, Expression arrayVarRefExpr, Expression indexExpr) {
        super(null, arrayVarRefExpr);
        this.symbolName = symbolName;
        this.indexExpr = indexExpr;
    }

    public SymbolName getSymbolName() {
        return symbolName;
    }

    public Expression getIndexExpr() {
        return indexExpr;
    }

    public boolean isLHSExpr() {
        return isLHSExpr;
    }

    public void setLHSExpr(boolean lhsExpr) {
        isLHSExpr = lhsExpr;
    }

    @Override
    public void accept(NodeVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * {@code ArrayAccessExprBuilder} represents an array access expression builder
     *
     * @since 1.0.0
     */
    public static class ArrayAccessExprBuilder {
        private SymbolName varName;
        private Expression arrayVarRefExpr;
        private Expression indexExpr;

        public ArrayAccessExprBuilder() {
        }

        public void setVarName(SymbolName varName) {
            this.varName = varName;
        }

        public void setArrayVarRefExpr(Expression arrayVarRefExpr) {
            this.arrayVarRefExpr = arrayVarRefExpr;
        }

        public void setIndexExpr(Expression rExpr) {
            this.indexExpr = rExpr;
        }

        public ArrayAccessExpr build() {
            return new ArrayAccessExpr(varName, arrayVarRefExpr, indexExpr);
        }
    }
}