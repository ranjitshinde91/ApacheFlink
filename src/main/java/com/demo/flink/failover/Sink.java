package com.demo.flink.failover;

import java.io.Serializable;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class Sink extends RichSinkFunction<String> implements Checkpointed<Sink.State>,CheckpointListener{

	private String state = "default";
	private String nonState= "default";
	private int counter = 0;
	@Override
	public void invoke(String value) throws Exception {
		counter++;
		System.out.println("message "+value+ " state: "+state+ " non state "+nonState);
		if(counter == 60){
			throw new MyException("Sink Failed Intentionally");
		}
		this.state = value;
		this.nonState = value;
		Thread.sleep(20);
	}
	

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		System.out.println("open() called");
	}


	@Override
	public void close() throws Exception {
		System.out.println("close() called");
		super.close();
	}


	@Override
	public State snapshotState(long checkpointId, long checkpointTimestamp)throws Exception {
		System.out.println("checkpointing initalied: snapshot storing state "+this.state);
		return new State(this.state);
	}

	@Override
	public void restoreState(State state) throws Exception {
		System.out.println("recovery started:state restored "+ state.getState());
		this.state = state.getState();
	}
	
	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		System.out.println("checkpointing complete: notifyCheckpointComplete "+checkpointId);
	}
	
	static class State implements Serializable{
		private String state;
		
		public State(String state){
			this.state= state;
		}

		public String getState() {
			return state;
		}

		public void setState(String state) {
			this.state = state;
		}
	}
}
