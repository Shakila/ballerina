/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.ballerinalang.nativeimpl.log;

import org.ballerinalang.bre.Context;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.values.BValue;
import org.ballerinalang.natives.AbstractNativeFunction;
import org.ballerinalang.natives.annotations.Argument;
import org.ballerinalang.natives.annotations.Attribute;
import org.ballerinalang.natives.annotations.BallerinaAnnotation;
import org.ballerinalang.natives.annotations.BallerinaFunction;

/**
 * Native function ballerina.log:debug
 *
 * @since 0.89
 */
@BallerinaFunction(
        packageName = "ballerina.log",
        functionName = "debug",
        args = {@Argument(name = "value", type = TypeKind.ANY)},
        isPublic = true
)
@BallerinaAnnotation(annotationName = "Description", attributes = {@Attribute(name = "value",
                                                                              value = "Logs the specified value at " +
                                                                                      "debug level.")})
@BallerinaAnnotation(annotationName = "Param", attributes = {@Attribute(name = "value",
                                                                        value = "The value to be logged.")})
public class LogDebug extends AbstractNativeFunction {

    public BValue[] execute(Context ctx) {
        BallerinaLogHandler.getLogger(ctx).debug(getRefArgument(ctx, 0).stringValue());
        return VOID_RETURN;
    }
}
