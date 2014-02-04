/**
 * 
 */
package org.cg.service.data;

import org.eclipse.core.runtime.CoreException;

import org.cg.dao.core.api.Service;

/**
 * @author Yanlin Wang (yanlinw@yahoo.com)
 *
 */
public class DataAppService implements Service {

	/**
	 * 
	 */
	public DataAppService() {
		// TODO Auto-generated constructor stub
	}

	/* (non-Javadoc)
	 * @see com.paysimple.appserver.core.api.Service#start()
	 */
	@Override
	public void start() throws CoreException {
		// TODO Auto-generated method stub
		PsKijiRESTManager.getInstance().start();	
	}

	/* (non-Javadoc)
	 * @see com.paysimple.appserver.core.api.Service#stop()
	 */
	@Override
	public void stop() throws CoreException {
		// TODO Auto-generated method stub
		PsKijiRESTManager.getInstance().stop();	
	}

}
