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

package org.apache.pig.impl.physicalLayer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.eval.EvalSpec;


public class POSort extends PhysicalOperator {
	static final long serialVersionUID = 1L; 
	EvalSpec sortSpec;
	transient Iterator<Tuple> iter;
	
	
	public POSort(EvalSpec sortSpec, int outputType) {
		super(outputType);
		this.sortSpec = sortSpec;
		this.inputs = new PhysicalOperator[1];
	}

	@Override
	public boolean open(boolean continueFromLast) throws IOException {
		if (!super.open(continueFromLast))
			return false;
		DataBag bag = BagFactory.getInstance().getNewBag();
		
		bag.sort(sortSpec);
		Tuple t;
		while((t = inputs[0].getNext())!=null){
			bag.add(t);
		}
		iter = bag.content();
		return true;
	}
	
	@Override
	public Tuple getNext() throws IOException {
		if (iter.hasNext())
			return iter.next();
		else
			return null;
	}

}
