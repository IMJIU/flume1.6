package com.im.flume.t02_failover;

import java.nio.charset.Charset;
import java.util.Properties;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;

class FailOverRpcClientFacade {

	private RpcClient client;
	
	private Properties props = new Properties();

	public void init() {
		// Setup properties for the failover
		Properties props = new Properties();
		props.put("client.type", "default_failover");

		// List of hosts (space-separated list of user-chosen host aliases)
		props.put("hosts", "h1 h2 h3");

		// host/port pair for each host alias
		String host1 = "192.168.199.210:41414";
		String host2 = "192.168.199.210:41415";
		String host3 = "192.168.199.210:41416";
		props.put("hosts.h1", host1);
		props.put("hosts.h2", host2);
		props.put("hosts.h3", host3);

		this.client =  RpcClientFactory.getInstance(props);
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