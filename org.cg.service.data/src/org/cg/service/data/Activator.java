/**
 * 
 */
package org.cg.service.data;

import org.osgi.framework.BundleContext;


/**
 * @author Yanlin Wang (yanlinw@yahoo.com)
 *
 */
public class Activator extends org.eclipse.core.runtime.Plugin {

	private volatile static Activator activator;

	private BundleContext ctx;

	@Override
	public void start(BundleContext arg0) throws Exception {
		// TODO Auto-generated method stub
		super.start(arg0);
		ctx = arg0;
		activator = this;
	}

	@Override
	public void stop(BundleContext arg0) throws Exception {
		// TODO Auto-generated method stub

		ctx = null;
		activator  = null;
		super.stop(arg0);
	}

	public BundleContext getBundleContext() {
		return ctx;
	}

	public static Activator getDefault() {
		return activator;
	}

}
