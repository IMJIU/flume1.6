package com.im.flume.t04_transaction;

import java.nio.charset.Charset;

import org.apache.flume.Channel;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.event.EventBuilder;

public class MyApp {

	public static void main(String[] args) {
		MyRpcClientFacade client = new MyRpcClientFacade();
		// Initialize client with the remote Flume agent's host and port
		client.init("192.168.199.210", 41414);
		// client.init();

		// Send 10 events to the remote Flume agent. That agent should be
		// configured to listen with an AvroSource.
		for (int i = 0; i < 10; i++) {
			Channel ch = new MemoryChannel();
			Transaction txn = ch.getTransaction();
			txn.begin();
			try {
				// This try clause includes whatever Channel operations you want to do

				Event eventToStage = EventBuilder.withBody("Hello Flume!", Charset.forName("UTF-8"));
				ch.put(eventToStage);
				// Event takenEvent = ch.take();
				// ...
				txn.commit();
			} catch (Throwable t) {
				txn.rollback();

				// Log exception, handle individual exceptions as needed

				// re-throw all Errors
				if (t instanceof Error) {
					throw (Error) t;
				}
			} finally {
				txn.close();
			}
		}

		client.cleanUp();
	}
}
