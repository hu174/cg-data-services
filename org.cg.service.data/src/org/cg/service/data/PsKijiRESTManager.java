/**
 * 
 */
package org.cg.service.data;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.core.runtime.FileLocator;
import org.eclipse.core.runtime.Path;
import org.kiji.rest.ManagedKijiClient;
import org.kiji.rest.serializers.AvroToJsonStringSerializer;
import org.kiji.rest.serializers.TableLayoutToJsonSerializer;
import org.kiji.rest.serializers.Utf8ToJsonSerializer;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiURI;
import org.osgi.framework.Bundle;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.Sets;
import com.yammer.dropwizard.json.ObjectMapperFactory;

/**
 * @author Yanlin Wang (yanlinw@yahoo.com)
 * 
 */
public class PsKijiRESTManager {
	public static final String CFG_DIR = "/conf";
	public static final String CFG_PROP = "conf.prop";
	public static final String PROP_INSTANCES = "instances";
	public static final String PROP_CLUSTER = "cluster";
	public static final String PROP_COREPOOLSIZE = "corePoolSize";
	public static final String PROP_MAXIMUNPOOLSIZE = "maximumPoolSize";
	public static final String PROP_KEEPALIVETIME = "keepAliveTime";
	public static final String PROP_QUEENSIZE = "queueSize";
	public static final String PROP_TABLE = "table";
	public static final String PROP_TABLELAYOUT = "tableLayout";
	public static final String PROP_INSTANCE = "instance";
	
	private static final Log _log = LogFactory.getLog(PsKijiRESTManager.class);
	private static volatile PsKijiRESTManager _instance;
	private ObjectMapperFactory objectMapperFactory;
	private static Properties properties;
	
	ManagedKijiClient kijiClient;

	private PsKijiRESTManager() {
		objectMapperFactory = new ObjectMapperFactory();
		registerSerializers(objectMapperFactory);
	}

	public static final void registerSerializers(
			ObjectMapperFactory mapperFactory) {
		// TODO: Add a module to convert btw Avro's specific types and JSON. The
		// default
		// mapping seems to throw an exception.
		SimpleModule module = new SimpleModule("KijiRestModule", new Version(1,
				0, 0, null, "org.kiji.rest", "avroToJson"));
		module.addSerializer(new AvroToJsonStringSerializer());
		module.addSerializer(new Utf8ToJsonSerializer());
		module.addSerializer(new TableLayoutToJsonSerializer());
		mapperFactory.registerModule(module);
	}
	
	public Properties getProperties(){
		return properties;
	}
	
	private static void setProperties(PropertiesConfiguration prop){
		properties = new Properties();
		properties.put(PROP_COREPOOLSIZE,prop.getInt(PROP_COREPOOLSIZE));
		properties.put(PROP_INSTANCE,prop.getString(PROP_INSTANCE));
		properties.put(PROP_KEEPALIVETIME,prop.getLong(PROP_KEEPALIVETIME));
		properties.put(PROP_MAXIMUNPOOLSIZE,prop.getInt(PROP_MAXIMUNPOOLSIZE));
		properties.put(PROP_QUEENSIZE,prop.getInt(PROP_QUEENSIZE));
		properties.put(PROP_TABLE,prop.getString(PROP_TABLE));
		properties.put(PROP_TABLELAYOUT,prop.getString(PROP_TABLELAYOUT));
	}
	
	public static PsKijiRESTManager getInstance() {
		if (_instance != null)
			return _instance;
		synchronized (PsKijiRESTManager.class) {
			if (_instance != null)
				return _instance;
			try {
				_instance = new PsKijiRESTManager();
				Bundle bundle = Activator.getDefault().getBundle();
				URL url = bundle.getEntry(CFG_DIR);
				String ipath = FileLocator.toFileURL(FileLocator.resolve(url))
						.getFile();
				String path = new Path(ipath).toOSString();

				PropertiesConfiguration prop = new PropertiesConfiguration(path
						+ File.separator + CFG_PROP);
				setProperties(prop);
				String[] instanceStrings = prop.getStringArray(PROP_INSTANCES);
				final Set<KijiURI> instances = Sets.newHashSet();
				String cluster_url_string = prop.getString(PROP_CLUSTER);
				final KijiURI clusterURI = KijiURI.newBuilder(
						cluster_url_string).build();

				for (String instance : instanceStrings) {
					final KijiURI instanceURI = KijiURI.newBuilder(clusterURI)
							.withInstanceName(instance).build();
					// Check existence of instance by opening and closing.
					final Kiji kiji = Kiji.Factory.open(instanceURI);
					kiji.release();
					instances.add(instanceURI);
					// environment.addHealthCheck(new
					// InstanceHealthCheck(instanceURI));
				}

				_instance.kijiClient = new ManagedKijiClient(clusterURI,
						instances);
				_instance.start();
			} catch (Exception e) {
				_log.error(e);

			}
		}
		return _instance;
	}

	public void start() {
		try {
			
			kijiClient.start();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void stop() {
		try {
			kijiClient.stop();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public ManagedKijiClient getKijiClient() {
		return kijiClient;
	}
	
	public ObjectMapperFactory getObjectMapperFactory(){
		return objectMapperFactory;
	}

	public ObjectMapper getObjectMapper(){
		return getObjectMapperFactory().build();
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
