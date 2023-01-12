/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateAction;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionInput;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionResponse;
import org.opensearch.dataprepper.plugins.processor.aggregate.GroupState;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.List;
import java.util.Map.Entry;

/**
 * An AggregateAction that combines multiple Events into a single Event. This action will add the unique keys of each smaller Event to the overall groupState,
 * and will create a combined Event from the groupState on concludeGroup. If smaller Events have the same keys, then these keys will be overwritten with the keys of the
 * most recently handled Event.
 * @since 1.3
 */
@DataPrepperPlugin(name = "merge_all", pluginType = AggregateAction.class)
public class MergeAllAggregateAction implements AggregateAction {
    static final String EVENT_TYPE = "event";

    @Override
    public AggregateActionResponse handleEvent(final Event event, final AggregateActionInput aggregateActionInput) {
        final GroupState groupState = aggregateActionInput.getGroupState();
        System.out.println(groupState.size());
        if (groupState.size() == 0) {
            // System.out.println("Adding first event");
            groupState.putAll(event.toMap());
        }
        // groupState.forEach((key, value) -> System.out.println(key + ":" + value));
        // event.getMetadata().getAttributes().forEach((key, value) -> System.out.println(key + ":" + value)); prints nothing
        System.out.println(event.getMetadata().toString());
        System.out.println(event.getMetadata().getEventType());
        System.out.println(event.getMetadata());
        Set<Entry<String, Object>> entrySet = event.toMap().entrySet();
        Iterator<Entry<String, Object>> iterator = entrySet.iterator();
        System.out.println();
        // System.out.println("Getting to iterator");
        while(iterator.hasNext()){
            Entry<String, Object> entry = iterator.next();
            String key = entry.getKey();
            Object value = entry.getValue();
            Object valueFromGroupState = groupState.getOrDefault(key, value);
            if (value != valueFromGroupState){
                System.out.println("Value is "+ value+" group state value is "+valueFromGroupState);
                List<Object> listValues = null;
                if (valueFromGroupState instanceof List) {
                    System.out.println("valueFromGroupState is instanceof List");
                    listValues = (List)valueFromGroupState;
                } else {
                    System.out.println("Initialising listvalues");
                    listValues = new ArrayList<>();
                    listValues.add(valueFromGroupState);
                }
                if (event.isValueAList(key)) {
                    System.out.println("value is instanceof List");
                    listValues.addAll(event.getList(key, Object.class));
                } else {
                    System.out.println("Adding value to list");
                    listValues.add(value);
                }
                groupState.put(key, listValues);
            }
        }
        return AggregateActionResponse.nullEventResponse();
    }

    @Override
    public Optional<Event> concludeGroup(final AggregateActionInput aggregateActionInput) {

        final Event event = JacksonEvent.builder()
                .withEventType(EVENT_TYPE)
                .withData(aggregateActionInput.getGroupState())
                .build();
        // TODO deduplicate listValues while concluding
        return Optional.of(event);
    }
}
