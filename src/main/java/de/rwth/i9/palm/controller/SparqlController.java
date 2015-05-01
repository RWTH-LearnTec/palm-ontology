package de.rwth.i9.palm.controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.reflections.Reflections;
import org.reflections.scanners.ResourcesScanner;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.ModelAndView;

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.ResultSet;

import de.rwth.i9.palm.helper.FileHelper;
import de.rwth.i9.palm.helper.ResultSetConverter;

@Controller
public class SparqlController extends TripleStore
{

	@RequestMapping( value = "/sparqlview", method = RequestMethod.GET )
	public ModelAndView snorqlIframe( @RequestParam( value = "sessionid", required = false ) final String sessionId, final HttpServletResponse response ) throws InterruptedException
	{
		ModelAndView model = new ModelAndView( "getSnorqlView" );

		if ( sessionId != null && sessionId.equals( "0" ) )
			response.setHeader( "SESSION_INVALID", "yes" );

		return model;
	}

	@RequestMapping( value = "/snorql", method = RequestMethod.GET )
	public ModelAndView snorql( @RequestParam( value = "sessionid", required = false ) final String sessionId, final HttpServletResponse response ) throws InterruptedException
	{
		ModelAndView model = new ModelAndView( "sparqlview", "link", "sparqlview" );

		if ( sessionId != null && sessionId.equals( "0" ) )
			response.setHeader( "SESSION_INVALID", "yes" );

		return model;
	}

	public String sparqlSelect( String q )
	{
		open();
		QueryExecution qe = null;
		String result;
		try
		{
			qe = createQueryExecution( q );

			ResultSet rs = qe.execSelect();
			System.out.println( "------------" + rs );
			result = ResultSetConverter.convertResultSetToJSON( rs );
			qe.close();
		}
		catch ( Exception e )
		{
			return ( e.getMessage() );
		}
		finally
		{
			if ( qe != null )
				qe.close();
		}

		return ( result );
	}

	/**
	 * this controller send the set of sparql queries used for integration
	 * testing.
	 * 
	 * @return
	 * @throws Exception
	 */
	@RequestMapping( value = "/sparqlview/sparql/queries", method = RequestMethod.GET )
	public @ResponseBody List<Map<String, String>> queries() throws Exception
	{
		List<Map<String, String>> result = new ArrayList<Map<String, String>>();
		Set<String> sparqls = new TreeSet<String>();
		sparqls.addAll( new Reflections( "sparql", new ResourcesScanner() ).getResources( Pattern.compile( "[^/]*\\.sparql" ) ) );
		Map<String, String> meta = new HashMap<String, String>();
		for ( String q : sparqls )
		{
			String query = FileHelper.getResourceAsString( q );
			if ( getMetaInfo( query ).get( "title" ) == null )
			{
				continue;
			}
			meta.put( "title", getMetaInfo( query ).get( "title" ) );
			meta.put( "query", query );
			meta.put( "tags", getMetaInfo( query ).get( "tags" ) );
			result.add( meta );
			meta = new HashMap<String, String>();
		}
		return result;
	}

	/**
	 * this controller eqecuted sparql query
	 * 
	 * @param request
	 * @param response
	 * @param query
	 * @param output
	 * @return
	 */
	@RequestMapping( value = "/sparqlview/sparql", produces = "application/json" )
	public @ResponseBody String sparql( HttpServletRequest request, HttpServletResponse response, @RequestParam( value = "query", required = false ) String query, @RequestParam( value = "output", required = false ) String output )
	{

		// default
		response.setHeader( "Accept", "application/sparql-results+json" );
		if ( output != null && output.equalsIgnoreCase( "xml" ) )
		{
			response.setHeader( "Accept", "application/sparql-results+xml" );
		}
		return sparqlSelect( query );
	}
}
