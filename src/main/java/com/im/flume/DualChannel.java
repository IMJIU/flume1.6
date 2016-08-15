package com.im.flume;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.flume.Event;
import org.apache.flume.channel.MemoryChannel;

public class DualChannel extends MemoryChannel{
	/***
	* putToMemChannel indicate put event to memChannel or fileChannel
	* takeFromMemChannel indicate take event from memChannel or fileChannel
	* */
	private AtomicBoolean putToMemChannel = new AtomicBoolean(true);
	private AtomicBoolean takeFromMemChannel = new AtomicBoolean(true);
	
	private MemoryChanne

	void doPut(Event event) {
	        if (switchon && putToMemChannel.get()) {
	              //往memChannel中写数据
	              memTransaction.put(event);

	              if ( memChannel.isFull() || fileChannel.getQueueSize() > 100) {
	                putToMemChannel.set(false);
	              }
	        } else {
	              //往fileChannel中写数据
	              fileTransaction.put(event);
	        }
	  }

	Event doTake() {
	    Event event = null;
	    if ( takeFromMemChannel.get() ) {
	        //从memChannel中取数据
	        event = memTransaction.take();
	        if (event == null) {
	            takeFromMemChannel.set(false);
	        } 
	    } else {
	        //从fileChannel中取数据
	        event = fileTransaction.take();
	        if (event == null) {
	            takeFromMemChannel.set(true);

	            putToMemChannel.set(true);
	        } 
	    }
	    return event;
	}
}

