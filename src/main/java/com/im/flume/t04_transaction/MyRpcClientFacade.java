package com.im.flume.t04_transaction;

import java.nio.charset.Charset;
import java.util.Properties;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;

class MyRpcClientFacade {

	private RpcClient client;

	private String hostname;

	private int port;

	public void init() {
		// Setup the RPC connection
		Properties props = new Properties();
		props.put("client.type", "default");
		props.put("hosts", "h1");
		props.put("hosts.h1", "192.168.199.210:41414");
		props.put("batch-size", "10");
		props.put("connect-timeout", "20000");
		props.put("request-timeout", "20000");
		this.client = RpcClientFactory.getInstance(props);
		// Use the following method to create a thrift client (instead of the above line):
		// this.client = RpcClientFactory.getThriftInstance(hostname, port);
	}
	public void init(String hostname, int port) {
		// Setup the RPC connection
		this.hostname = hostname;
		this.port = port;
		this.client = RpcClientFactory.getDefaultInstance(hostname, port);
		// Use the following method to create a thrift client (instead of the above line):
		// this.client = RpcClientFactory.getThriftInstance(hostname, port);
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
			client = RpcClientFactory.getDefaultInstance(hostname, port);
			// Use the following method to create a thrift client (instead of the above line):
			// this.client = RpcClientFactory.getThriftInstance(hostname, port);
		}
	}

	public void cleanUp() {
		// Close the RPC connection
		client.close();
	}

}