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

package org.apache.flink.table.runtime.operators.aggregate;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.JoinedRow;
import org.apache.flink.table.runtime.context.ExecutionContext;
import org.apache.flink.table.runtime.dataview.PerKeyStateDataViewStore;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.operators.bundle.MapBundleFunction;
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

import java.util.Map;

import static org.apache.flink.table.dataformat.util.BaseRowUtil.ACCUMULATE_MSG;
import static org.apache.flink.table.dataformat.util.BaseRowUtil.RETRACT_MSG;

/**
 * Aggregate Function used for the global groupby (without window) aggregate in miniBatch mode.
 */
public class MiniBatchGlobalGroupAggFunction extends MapBundleFunction<BaseRow, BaseRow, BaseRow, BaseRow> {


	private static final long serialVersionUID = 8349579876002001744L;

	/**
	 * The code generated local function used to handle aggregates.
	 */
	private final GeneratedAggsHandleFunction genLocalAggsHandler;

	/**
	 * The code generated global function used to handle aggregates.
	 */
	private final GeneratedAggsHandleFunction genGlobalAggsHandler;

	/**
	 * The code generated equaliser used to equal BaseRow.
	 */
	private final GeneratedRecordEqualiser genRecordEqualiser;

	/**
	 * The accumulator types.
	 */
	private final LogicalType[] accTypes;

	/**
	 * Used to count the number of added and retracted input records.
	 */
	private final RecordCounter recordCounter;

	/**
	 * Whether this operator will generate retraction.
	 */
	private final boolean generateRetraction;

	/**
	 * Reused output row.
	 */
	private transient JoinedRow resultRow = new JoinedRow();

	// local aggregate function to handle local combined accumulator rows
	private transient AggsHandleFunction localAgg = null;

	// global aggregate function to handle global accumulator rows
	private transient AggsHandleFunction globalAgg = null;

	// function used to equal BaseRow
	private transient RecordEqualiser equaliser = null;

	// stores the accumulators
	private transient ValueState<BaseRow> accState = null;


	/**
	 * Creates a {@link MiniBatchGlobalGroupAggFunction}.
	 *
	 * @param genLocalAggsHandler The generated local aggregate handler
	 * @param genGlobalAggsHandler The generated global aggregate handler
	 * @param genRecordEqualiser The code generated equaliser used to equal BaseRow.
	 * @param accTypes The accumulator types.
	 * @param indexOfCountStar The index of COUNT(*) in the aggregates.
	 *                          -1 when the input doesn't contain COUNT(*), i.e. doesn't contain retraction messages.
	 *                          We make sure there is a COUNT(*) if input stream contains retraction.
	 * @param generateRetraction Whether this operator will generate retraction.
	 */
	public MiniBatchGlobalGroupAggFunction(
			GeneratedAggsHandleFunction genLocalAggsHandler,
			GeneratedAggsHandleFunction genGlobalAggsHandler,
			GeneratedRecordEqualiser genRecordEqualiser,
			LogicalType[] accTypes,
			int indexOfCountStar,
			boolean generateRetraction) {
		this.genLocalAggsHandler = genLocalAggsHandler;
		this.genGlobalAggsHandler = genGlobalAggsHandler;
		this.genRecordEqualiser = genRecordEqualiser;
		this.accTypes = accTypes;
		this.recordCounter = RecordCounter.of(indexOfCountStar);
		this.generateRetraction = generateRetraction;
	}

	@Override
	public void open(ExecutionContext ctx) throws Exception {
		super.open(ctx);
		localAgg = genLocalAggsHandler.newInstance(ctx.getRuntimeContext().getUserCodeClassLoader());
		localAgg.open(new PerKeyStateDataViewStore(ctx.getRuntimeContext()));

		globalAgg = genGlobalAggsHandler.newInstance(ctx.getRuntimeContext().getUserCodeClassLoader());
		globalAgg.open(new PerKeyStateDataViewStore(ctx.getRuntimeContext()));

		equaliser = genRecordEqualiser.newInstance(ctx.getRuntimeContext().getUserCodeClassLoader());

		BaseRowTypeInfo accTypeInfo = new BaseRowTypeInfo(accTypes);
		ValueStateDescriptor<BaseRow> accDesc = new ValueStateDescriptor<>("accState", accTypeInfo);
		accState = ctx.getRuntimeContext().getState(accDesc);

		resultRow = new JoinedRow();
	}

	/**
	 * The {@code previousAcc} is accumulator, but input is a row in &lt;key, accumulator&gt; schema,
	 * the specific generated {@link #localAgg} will project the {@code input} to accumulator
	 * in merge method.
	 */
	@Override
	public BaseRow addInput(@Nullable BaseRow previousAcc, BaseRow input) throws Exception {
		BaseRow currentAcc;
		if (previousAcc == null) {
			currentAcc = localAgg.createAccumulators();
		} else {
			currentAcc = previousAcc;
		}

		localAgg.setAccumulators(currentAcc);
		localAgg.merge(input);
		return localAgg.getAccumulators();
	}

	@Override
	public void finishBundle(Map<BaseRow, BaseRow> buffer, Collector<BaseRow> out) throws Exception {
		for (Map.Entry<BaseRow, BaseRow> entry : buffer.entrySet()) {
			BaseRow currentKey = entry.getKey();
			BaseRow bufferAcc = entry.getValue();

			boolean firstRow = false;

			// set current key to access states under the current key
			ctx.setCurrentKey(currentKey);
			BaseRow stateAcc = accState.value();
			if (stateAcc == null) {
				stateAcc = globalAgg.createAccumulators();
				firstRow = true;
			}
			// set accumulator first
			globalAgg.setAccumulators(stateAcc);
			// get previous aggregate result
			BaseRow prevAggValue = globalAgg.getValue();

			// merge bufferAcc to stateAcc
			globalAgg.merge(bufferAcc);
			// get current aggregate result
			BaseRow newAggValue = globalAgg.getValue();
			// get new accumulator
			stateAcc = globalAgg.getAccumulators();

			if (!recordCounter.recordCountIsZero(stateAcc)) {
				// we aggregated at least one record for this key

				// update acc to state
				accState.update(stateAcc);

				// if this was not the first row and we have to emit retractions
				if (!firstRow) {
					if (!equaliser.equalsWithoutHeader(prevAggValue, newAggValue)) {
						// new row is not same with prev row
						if (generateRetraction) {
							// prepare retraction message for previous row
							resultRow.replace(currentKey, prevAggValue).setHeader(RETRACT_MSG);
							out.collect(resultRow);
						}
						// prepare accumulation message for new row
						resultRow.replace(currentKey, newAggValue).setHeader(ACCUMULATE_MSG);
						out.collect(resultRow);
					}
					// new row is same with prev row, no need to output
				} else {
					// this is the first, output new result
					// prepare accumulation message for new row
					resultRow.replace(currentKey, newAggValue).setHeader(ACCUMULATE_MSG);
					out.collect(resultRow);
				}

			} else {
				// we retracted the last record for this key
				// sent out a delete message
				if (!firstRow) {
					// prepare delete message for previous row
					resultRow.replace(currentKey, prevAggValue).setHeader(RETRACT_MSG);
					out.collect(resultRow);
				}
				// and clear all state
				accState.clear();
				// cleanup dataview under current key
				globalAgg.cleanup();
			}
		}
	}

	@Override
	public void close() throws Exception {
		if (localAgg != null) {
			localAgg.close();
		}
		if (globalAgg != null) {
			globalAgg.close();
		}
	}
}
