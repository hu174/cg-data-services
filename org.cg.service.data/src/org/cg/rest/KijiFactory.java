package org.cg.rest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.kiji.rest.KijiClient;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiURI;
import org.kiji.schema.hbase.HBaseFactory;
import org.kiji.schema.impl.HBaseKiji;

public class KijiFactory {
	public static Kiji open(KijiClient kijiClient, String instance) {
		Kiji kiji = kijiClient.getKiji(instance);
		return kiji;
	}

}
