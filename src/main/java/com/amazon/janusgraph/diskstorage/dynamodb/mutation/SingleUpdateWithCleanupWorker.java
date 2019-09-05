/*
 * Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package com.amazon.janusgraph.diskstorage.dynamodb.mutation;

import java.util.Map;

import com.amazon.janusgraph.diskstorage.dynamodb.AsyncTask;

import com.amazon.janusgraph.diskstorage.dynamodb.Constants;
import com.amazon.janusgraph.diskstorage.dynamodb.DynamoDbDelegate;
import com.amazon.janusgraph.diskstorage.dynamodb.ExponentialBackoff.DeleteItem;
import com.amazon.janusgraph.diskstorage.dynamodb.ExponentialBackoff.UpdateItem;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;

import lombok.RequiredArgsConstructor;

/**
 *
 * @author Alexander Patrikalakis
 * @author Michael Rodaitis
 *
 */
@RequiredArgsConstructor
public class SingleUpdateWithCleanupWorker extends MutateWorker {

    private static final int ATTRIBUTES_IN_EMPTY_SINGLE_ITEM = 1;

    private final UpdateItemRequest updateItemRequest;
    private final DynamoDbDelegate dynamoDbDelegate;


    @Override
    public AsyncTask<Void> callAsync() {

        final UpdateItem updateBackoff = new UpdateItem(updateItemRequest, dynamoDbDelegate);
        final AsyncTask<UpdateItemResult> result = updateBackoff.runWithBackoffAsync();

        return result.flatMap(r -> {
            final Map<String, AttributeValue> item = r.getAttributes();

            // If the record has no Titan columns left after deletions occur, then just delete the record
            if (item != null && item.containsKey(Constants.JANUSGRAPH_HASH_KEY) && item.size() == ATTRIBUTES_IN_EMPTY_SINGLE_ITEM) {
                final DeleteItem deleteBackoff = new DeleteItem(new DeleteItemRequest().withTableName(updateItemRequest.getTableName())
                .withKey(updateItemRequest.getKey()), dynamoDbDelegate);
                return AsyncTask.allOf(deleteBackoff.runWithBackoffAsync());
            }

            return AsyncTask.completedFuture(null);
        });

    }
}
