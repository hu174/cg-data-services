/**
 * 
 */
package org.cg.app.core.service;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

/**
 * @author yanlin wang (yanlinw@yahoo.com)
 * 
 */
@Path("/core-service")
public class CoreService {

	public CoreService() {
	}

	@GET
	@Path("/ping/{name}")
	@Produces({MediaType.TEXT_PLAIN , MediaType.TEXT_XML, MediaType.APPLICATION_JSON} )
	public String ping (@PathParam("name") String name) {
		return name;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
	}

}
