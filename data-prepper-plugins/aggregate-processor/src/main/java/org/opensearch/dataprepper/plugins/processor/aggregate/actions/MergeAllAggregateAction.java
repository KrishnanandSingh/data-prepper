/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateAction;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionInput;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionResponse;
import org.opensearch.dataprepper.plugins.processor.aggregate.GroupState;

/**
 * An AggregateAction that combines multiple Events into a single Event. This
 * action will add the unique keys of each smaller Event to the overall
 * groupState,
 * and will create a combined Event from the groupState on concludeGroup. If
 * smaller Events have the same keys, then these keys will be overwritten with
 * the keys of the
 * most recently handled Event.
 * 
 * @since 1.3
 */
@DataPrepperPlugin(name = "merge_all", pluginType = AggregateAction.class)
public class MergeAllAggregateAction implements AggregateAction {
    static final String EVENT_TYPE = "event";
    public final Map<String, String> dataTypeMap = new HashMap<String, String>();

    // @DataPrepperPluginConstructor
    // public MergeAllAggregateAction(final MergeAllAggregateActionConfig
    // mergeAllAggregateActionConfig) {
    // this.dataTypeMap = mergeAllAggregateActionConfig.getDataTypes();
    // }

    /*
     * (non-Javadoc)
     * 
     * @see org.opensearch.dataprepper.plugins.processor.aggregate.AggregateAction#
     * handleEvent(org.opensearch.dataprepper.model.event.Event,
     * org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionInput)
     */
    @Override
    public AggregateActionResponse handleEvent(final Event event, final AggregateActionInput aggregateActionInput) {
        final GroupState groupState = aggregateActionInput.getGroupState();
        System.out.println(groupState.size());
        dataTypeMap.put("output_snmp", "integer");
        dataTypeMap.put("flow_seq_num", "integer");
        dataTypeMap.put("src_tos", "integer");
        dataTypeMap.put("input_snmp", "integer");
        dataTypeMap.put("l4_dst_port", "integer");
        dataTypeMap.put("tcp_flags", "integer");
        dataTypeMap.put("in_bytes", "integer");
        dataTypeMap.put("in_pkts", "integer");
        dataTypeMap.put("protocol", "integer");
        dataTypeMap.put("flowset_id", "integer");
        dataTypeMap.put("version", "integer");
        dataTypeMap.put("dst_as", "integer");
        dataTypeMap.put("ip_dscp", "integer");
        dataTypeMap.put("l4_src_port", "integer");
        dataTypeMap.put("ipv4_src_addr", "string");
        dataTypeMap.put("first_switched", "string");
        dataTypeMap.put("last_switched", "string");
        dataTypeMap.put("ipv4_dst_addr", "string");
        
        if (groupState.size() == 0) {
            groupState.putAll(event.toMap());
        }
        // dataTypeMap.forEach((key, value) -> System.out.println(key + ":" + value));
        System.out.println();
       
        Set<String> eventKeys = event.toMap().keySet();
        for (String key : eventKeys) {
            if (!dataTypeMap.containsKey(key)) {
                System.out.println("Skipping " + key);
                continue;
            }
            System.out.println("Checking key: " + key);
            Object value = null;
            String dataType = dataTypeMap.get(key);
            switch (dataType) {
                case "integer":
                    value = event.get(key, Integer.class);
                    break;

                case "string":
                    value = event.get(key, String.class);
                    break;
            }

            Object valueFromGroupState = groupState.getOrDefault(key, value);
            List<Object> listValues = null;
            if (valueFromGroupState instanceof List) {
                listValues = (List) valueFromGroupState;
                listValues.add(value);
            } else {
                if (!value.equals(valueFromGroupState)) {
                    listValues = new ArrayList<>();
                    listValues.add(valueFromGroupState);
                    listValues.add(value);
                    groupState.put(key, listValues);
                }
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
