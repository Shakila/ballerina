/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.ballerinalang.nativeimpl.actions.data.sql.client;


import org.ballerinalang.bre.Context;
import org.ballerinalang.connector.api.ConnectorFuture;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.values.BConnector;
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BString;
import org.ballerinalang.model.values.BStruct;
import org.ballerinalang.nativeimpl.actions.ClientConnectorFuture;
import org.ballerinalang.nativeimpl.actions.data.sql.Constants;
import org.ballerinalang.nativeimpl.actions.data.sql.SQLDatasource;
import org.ballerinalang.natives.annotations.Argument;
import org.ballerinalang.natives.annotations.BallerinaAction;

/**
 * {@code Init} is the Init action implementation of the SQL Connector.
 *
 * @since 0.8.5
 */
@BallerinaAction(
        packageName = "ballerina.data.sql",
        actionName = "<init>",
        connectorName = Constants.CONNECTOR_NAME,
        args = {@Argument(name = "c", type = TypeKind.CONNECTOR)
        },
        connectorArgs = {
                @Argument(name = "options", type = TypeKind.MAP)
        })
public class Init extends AbstractSQLAction {

    @Override
    public ConnectorFuture execute(Context context) {
        BConnector bConnector = (BConnector) getRefArgument(context, 0);
        BStruct optionStruct = (BStruct) bConnector.getRefField(0);
        BMap sharedMap = (BMap) bConnector.getRefField(1);
        String dbType = bConnector.getStringField(0);
        String hostOrPath = bConnector.getStringField(1);
        String dbName = bConnector.getStringField(2);
        String username = bConnector.getStringField(3);
        String password = bConnector.getStringField(4);
        int port = (int) bConnector.getIntField(0);
        if (sharedMap.get(new BString(Constants.DATASOURCE_KEY)) == null) {
            SQLDatasource datasource = new SQLDatasource();
            datasource.init(optionStruct, dbType, hostOrPath, port, username, password, dbName);
            sharedMap.put(new BString(Constants.DATASOURCE_KEY), datasource);
        }
        ClientConnectorFuture future = new ClientConnectorFuture();
        future.notifySuccess();
        return future;
    }

}
