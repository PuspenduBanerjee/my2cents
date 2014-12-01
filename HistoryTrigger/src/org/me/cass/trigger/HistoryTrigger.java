package org.me.cass.trigger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.triggers.ITrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HistoryTrigger implements ITrigger {
	private static final Logger LOGGER=LoggerFactory.getLogger(HistoryTrigger.class);
	static{
		LOGGER.info("Loaded "+HistoryTrigger.class.getName());
	}
	private static final String HISTORY_SUFFIX="_history";
	@Override
	public Collection<RowMutation> augment(ByteBuffer key, ColumnFamily update) {
		String keyspace=update.metadata().ksName;
		String columnFamily=update.metadata().cfName;
		LOGGER.warn("Triggered KS.CF: "+keyspace+"."+columnFamily); 
		//TODO: OnDemand File backed List Implementation 
		List<RowMutation> mutations = new ArrayList<RowMutation>();
			//LOGGER.warn("****key: "+new String(key.array()));
	        for (Column cell : update)
	        {
	        	// Skip the row marker and other empty values, since they lead to an empty key.
	            if (cell.value().remaining() > 0)
	            {
	            	LOGGER.info(new String(cell.name().array())
    					+"=>"+new String(cell.value().array()));
	            	RowMutation mutation = new RowMutation(keyspace, key);
	                mutation.add(columnFamily+HISTORY_SUFFIX, cell.name(), cell.value(), System.currentTimeMillis());
	                mutations.add(mutation);
	            }
	        }
	        return mutations;
	}

}
