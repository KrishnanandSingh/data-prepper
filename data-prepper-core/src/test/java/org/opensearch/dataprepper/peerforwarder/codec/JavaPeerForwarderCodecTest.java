package org.opensearch.dataprepper.peerforwarder.codec;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.peerforwarder.model.PeerForwardingEvents;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JavaPeerForwarderCodecTest {
    private static final String PLUGIN_ID = "plugin_id";
    private static final String PIPELINE_NAME = "pipeline_name";

    private final JavaPeerForwarderCodec objectUnderTest = new JavaPeerForwarderCodec();

    @Test
    void testCodec() throws IOException, ClassNotFoundException {
        final PeerForwardingEvents inputEvents = generatePeerForwardingEvents(2);
        final byte[] bytes = objectUnderTest.serialize(inputEvents);
        final PeerForwardingEvents outputEvents = objectUnderTest.deserialize(bytes);
        assertThat(outputEvents.getDestinationPipelineName(), equalTo(inputEvents.getDestinationPipelineName()));
        assertThat(outputEvents.getDestinationPluginId(), equalTo(inputEvents.getDestinationPluginId()));
        assertThat(outputEvents.getEvents().size(), equalTo(inputEvents.getEvents().size()));
    }

    @Test
    void testDeserializeException(){
        final byte[] bytes = new byte[0];
        assertThrows(IOException.class, () -> objectUnderTest.deserialize(bytes));
    }

    private PeerForwardingEvents generatePeerForwardingEvents(final int numEvents) {
        final List<Event> events = new ArrayList<>();
        for (int i = 0; i < numEvents; i++) {
            final Map<String, String> eventData = new HashMap<>();
            eventData.put("key1", "value");
            eventData.put("key2", "value");
            final JacksonEvent event = JacksonLog.builder().withData(eventData).withEventType("LOG").build();
            events.add(event);
        }
        return new PeerForwardingEvents(events, PLUGIN_ID, PIPELINE_NAME);
    }
}