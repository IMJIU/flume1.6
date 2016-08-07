package com.im.flume.t01;



public class MyApp {
	/**
	 * arvo type
	 * @param args
	 */
	public static void main(String[] args) {
		MyRpcClientFacade client = new MyRpcClientFacade();
		// Initialize client with the remote Flume agent's host and port
//		client.init("192.168.199.210", 5140);
		System.out.println("init");
		client.init();
		System.out.println("int end");
		// Send 10 events to the remote Flume agent. That agent should be
		// configured to listen with an AvroSource.
		String sampleData = "Hello Flume!";
		for (int i = 0; i < 10; i++) {
			client.sendDataToFlume(sampleData);
			System.out.println("send");
		}

		client.cleanUp();
	}
}
