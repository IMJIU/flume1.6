package com.im.flume.t05_embedded_agent;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.agent.embedded.EmbeddedAgent;

import com.google.common.collect.Lists;

public class MyApp {

	public static void main(String[] args) throws EventDeliveryException {

		Map<String, String> properties = new HashMap<String, String>();
		properties.put("channel.type", "memory");
		properties.put("channel.capacity", "200");
		properties.put("sinks", "sink1 sink2");
		properties.put("sink1.type", "avro");
		properties.put("sink2.type", "avro");
		properties.put("sink1.hostname", "collector1.apache.org");
		properties.put("sink1.port", "5564");
		properties.put("sink2.hostname", "collector2.apache.org");
		properties.put("sink2.port",  "5565");
		properties.put("processor.type", "load_balance");
		properties.put("source.interceptors", "i1");
		properties.put("source.interceptors.i1.type", "static");
		properties.put("source.interceptors.i1.key", "key1");
		properties.put("source.interceptors.i1.value", "value1");

		EmbeddedAgent agent = new EmbeddedAgent("myagent");

		agent.configure(properties);
		agent.start();

		List<Event> events = Lists.newArrayList();
		Event event = new Event() {
			
			@Override
			public void setHeaders(Map<String, String> arg0) {
				
			}
			
			@Override
			public void setBody(byte[] arg0) {
				
			}
			
			@Override
			public Map<String, String> getHeaders() {
				return null;
			}
			
			@Override
			public byte[] getBody() {
				return null;
			}
		};
		events.add(event);
		events.add(event);
		events.add(event);
		events.add(event);

		agent.putAll(events);

//		...

		agent.stop();
	}
}
