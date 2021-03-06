/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.util.SerializedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Simple bean to describe the state belonging to a parallel operator.
 * Since we hold the state across execution attempts, we identify a task by its
 * JobVertexId and subtask index.
 * 
 * The state itself is kept in serialized from, since the checkpoint coordinator itself
 * is never looking at it anyways and only sends it back out in case of a recovery.
 * Furthermore, the state may involve user-defined classes that are not accessible without
 * the respective classloader.
 */
public class StateForTask implements Serializable {

	private static final long serialVersionUID = -2394696997971923995L;

	private static final Logger LOG = LoggerFactory.getLogger(StateForTask.class);

	/** The state of the parallel operator */
	private final SerializedValue<StateHandle<?>> state;

	/**
	 * The state size. This is also part of the deserialized state handle.
	 * We store it here in order to not deserialize the state handle when
	 * gathering stats.
	 */
	private final long stateSize;

	/** The vertex id of the parallel operator */
	private final JobVertexID operatorId;
	
	/** The index of the parallel subtask */
	private final int subtask;

	/** The duration of the acknowledged (ack timestamp - trigger timestamp). */
	private final long duration;
	
	public StateForTask(
			SerializedValue<StateHandle<?>> state,
			long stateSize,
			JobVertexID operatorId,
			int subtask,
			long duration) {

		this.state = checkNotNull(state, "State");
		// Sanity check and don't fail checkpoint because of this.
		this.stateSize = stateSize >= 0 ? stateSize : 0;
		this.operatorId = checkNotNull(operatorId, "Operator ID");

		checkArgument(subtask >= 0, "Negative subtask index");
		this.subtask = subtask;

		this.duration = duration;
	}

	// --------------------------------------------------------------------------------------------
	
	public SerializedValue<StateHandle<?>> getState() {
		return state;
	}

	public long getStateSize() {
		return stateSize;
	}

	public JobVertexID getOperatorId() {
		return operatorId;
	}

	public int getSubtask() {
		return subtask;
	}

	public long getDuration() {
		return duration;
	}

	public void discard(ClassLoader userClassLoader) {
		try {
			state.deserializeValue(userClassLoader).discardState();
		} catch (Exception e) {
			LOG.warn("Failed to discard checkpoint state: " + this, e);
		}
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		else if (o instanceof StateForTask) {
			StateForTask that = (StateForTask) o;
			return this.subtask == that.subtask && this.operatorId.equals(that.operatorId)
					&& this.state.equals(that.state);
		}
		else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return state.hashCode() + 31 * operatorId.hashCode() + 43 * subtask;
	}

	@Override
	public String toString() {
		return String.format("StateForTask %s-%d : %s", operatorId, subtask, state);
	}
}
