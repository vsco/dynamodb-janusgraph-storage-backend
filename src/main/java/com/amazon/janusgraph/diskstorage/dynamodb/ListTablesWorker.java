/*
 * Copyright 2014-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazon.janusgraph.diskstorage.dynamodb;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.dynamodbv2.model.ListTablesRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;

import lombok.Getter;

/**
 * This class merges multiple pages of table names into one ListTablesResult.
 *
 * @author Alexander Patrikalakis
 */
public class ListTablesWorker extends PaginatingTask<ListTablesResult> {
    @Getter
    private final ListTablesRequest request = new ListTablesRequest();
    private final List<String> tableNames = new ArrayList<>();

    ListTablesWorker(final DynamoDbDelegate delegate) {
        super(delegate, delegate.getListTablesApiName(), null /*tableName*/);
    }

    @Override
    public AsyncTask<ListTablesResult> next() {
        final AsyncTask<ListTablesResult> result = delegate.listTablesAsync(request);
        return result.map(r -> {

            if (r.getLastEvaluatedTableName() != null && !r.getLastEvaluatedTableName().isEmpty()) {
                request.setExclusiveStartTableName(r.getLastEvaluatedTableName());
            } else { //done
                markComplete();
            }

            // c add scanned items
            tableNames.addAll(r.getTableNames());
            return r;
        });
    }

    @Override
    protected ListTablesResult getMergedPages() {
        return new ListTablesResult().withTableNames(tableNames);
    }
}
