package com.im.flume.t03_loadbalance;

import java.nio.charset.Charset;
import java.util.Properties;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;

class LoadBalanceRpcClientFacade {

	private RpcClient client;

	private Properties props = new Properties();

	public void init() {
		// Setup properties for the load balancing
		props.put("client.type", "default_loadbalance");

		// List of hosts (space-separated list of user-chosen host aliases)
		props.put("hosts", "h1 h2 h3");

		// host/port pair for each host alias
		String host1 = "host1.example.org:41414";
		String host2 = "host2.example.org:41414";
		String host3 = "host3.example.org:41414";
		props.put("hosts.h1", host1);
		props.put("hosts.h2", host2);
		props.put("hosts.h3", host3);

		props.put("host-selector", "random"); // For random host selection
		// props.put("host-selector", "round_robin"); // For round-robin host
		// // selection
		props.put("backoff", "true"); // Disabled by default.

		props.put("maxBackoff", "10000"); // Defaults 0, which effectively
		                                  // becomes 30000 ms

		// Create the client with load balancing properties
		client = RpcClientFactory.getInstance(props);
	}

	public void sendDataToFlume(String data) {
		// Create a Flume Event object that encapsulates the sample data
		Event event = EventBuilder.withBody(data, Charset.forName("UTF-8"));

		// Send the event
		try {
			client.append(event);
		} catch (EventDeliveryException e) {
			// clean up and recreate the client
			client.close();
			client = null;
			client = RpcClientFactory.getInstance(props);
			// Use the following method to create a thrift client (instead of the above line):
			// this.client = RpcClientFactory.getThriftInstance(hostname, port);
		}
	}

	public void cleanUp() {
		// Close the RPC connection
		client.close();
	}

}