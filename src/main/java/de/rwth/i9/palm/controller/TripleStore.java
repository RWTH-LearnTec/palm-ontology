package de.rwth.i9.palm.controller;


import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.beans.factory.annotation.Autowired;

import virtuoso.jena.driver.VirtGraph;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.engine.http.QueryEngineHTTP;

import de.rwth.i9.palm.service.ConfigurationService;

public class TripleStore
{
	@Autowired
	public ConfigurationService config;

	private static String endpoint;
	private Model model;
	private boolean isNative = false;

	//
	// identify the current test
	protected static String instanceSignature = "";
	private static String date = new SimpleDateFormat( "yyyyMMdd-HHmm" ).format( new Date() );

	private String prefix = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" + "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n" + "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" + "PREFIX dcterms: <http://purl.org/dc/terms/>\n" + "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n" + "PREFIX sim: <http://purl.org/ontology/similarity/>\n" + "PREFIX mo: <http://purl.org/ontology/mo/>\n" + "PREFIX ov: <http://open.vocab.org/terms/>\n" + "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + "PREFIX : <http://nextprot.org/rdf#>\n" + "PREFIX entry: <http://nextprot.org/rdf/entry/>\n" + "PREFIX isoform: <http://nextprot.org/rdf/isoform/>\n" + "PREFIX annotation: <http://nextprot.org/rdf/annotation/>\n" + "PREFIX evidence: <http://nextprot.org/rdf/evidence/>\n" + "PREFIX xref: <http://nextprot.org/rdf/xref/>\n" + "PREFIX publication: <http://nextprot.org/rdf/publication/>\n" + "PREFIX term: <http://nextprot.org/rdf/terminology/>\n" + "PREFIX gene: <http://nextprot.org/rdf/gene/>\n" + "PREFIX source: <http://nextprot.org/rdf/source/>\n" + "PREFIX db: <http://nextprot.org/rdf/db/>\n" + "PREFIX context: <http://nextprot.org/rdf/context/>\n";

	public void open()
	{

		if ( endpoint != null )
			return;

		//
		// trying to use virtuoso with the native driver
		if ( !config.getVirtuosoUrl().equals( "" ) )
		{

			VirtGraph graph = new VirtGraph( config.getVirtuosoGraph(), 
					config.getVirtuosoUrl(), 
					config.getVirtuosoUser(), 
					config.getVirtuosoPassword() );
			model = ModelFactory.createModelForGraph( graph );
			instanceSignature = generateTestId();
			isNative = true;
			return;
		}
		//
		// else we use the apsql endpoint (that doesn't support ARQ)
		String proxied = "\n";
		endpoint = config.getSparqlEndpoint();
		if ( !config.getSparqlProxy().equals( "" ) )
			proxied = " endpoint:" + config.getSparqlProxy() + "\n";

		instanceSignature = generateTestId();
	}

	/**
	 * generate meta content for sparql query logs
	 * 
	 * @return
	 */
	private String generateTestId()
	{
		String version = getTripleVersion() + "-";
		String testId = "SPARQL-" + version + date;
		return testId;
	}

	/**
	 * read meta info 'ac' in sparql query comment - endpoint of sparql service
	 * - id of the query - title of the query - ac variable define the Proteins
	 * we request as result - count variable define the size of the result
	 * 
	 * @param query
	 * @return
	 */

	public Map<String, String> getMetaInfo( String query )
	{
		Map<String, String> meta = new HashMap<String, String>();
		//
		// get id and host
		Matcher m = Pattern.compile( "#id:([^ ]+).?endpoint:([^\\n]*)", Pattern.DOTALL | Pattern.MULTILINE ).matcher( query );
		if ( m.find() )
		{
			meta.put( "id", m.group( 1 ) );
			meta.put( "endpoint", m.group( 2 ) );
		}

		//
		// get tags
		m = Pattern.compile( "[# ]?tags:([^ \\n]*)", Pattern.DOTALL | Pattern.MULTILINE ).matcher( query );
		if ( m.find() )
		{
			meta.put( "tags", m.group( 1 ) );
		}

		//
		// get acs
		m = Pattern.compile( "[# ]?ac:([^ \\n]*)", Pattern.DOTALL | Pattern.MULTILINE ).matcher( query );
		if ( m.find() )
		{
			meta.put( "acs", m.group( 1 ) );
		}

		//
		// get count
		m = Pattern.compile( "[# ]?count:([^\\n]*)", Pattern.DOTALL | Pattern.MULTILINE ).matcher( query );
		meta.put( "count", "0" );
		if ( m.find() )
		{
			meta.put( "count", m.group( 1 ) );
		}

		//
		// get title
		m = Pattern.compile( "#title:([^\\n]*)", Pattern.DOTALL | Pattern.MULTILINE ).matcher( query );
		if ( m.find() )
		{
			meta.put( "title", m.group( 1 ) );
		}
		return meta;
	}

	public int getQueryMetaCount( String query )
	{
		String c = getMetaInfo( query ).get( "count" );
		if ( c == null )
			return 0;
		return Integer.parseInt( c );
	}

	/**
	 * read meta info 'pending' in sparql query comment pending is the status of
	 * the query quality check
	 * 
	 * @param query
	 * @return
	 */
	public boolean isQueryPending( String query )
	{
		return query.indexOf( "#pending" ) != -1;
	}

	/**
	 * get a normalized list of accession
	 * 
	 * @param rs
	 * @return
	 */
	public List<String> getLiterals( ResultSet rs )
	{
		return getLiterals( rs, "entry", "NX_" );
	}

	/**
	 * get a list of literal
	 * 
	 * @param rs
	 * @param variable
	 * @param replace
	 * @return
	 */
	public List<String> getLiterals( ResultSet rs, String variable, String replace )
	{
		List<String> uri = new ArrayList<String>();
		while ( rs.hasNext() )
		{
			QuerySolution qs = rs.next();
			uri.add( qs.getResource( variable ).getLocalName().replace( replace, "" ) );
		}
		Collections.sort( uri );
		return uri;
	}

	public String getTripleVersion()
	{
		try
		{
			ResultSet rs = createQueryExecution( "select ?version where{ :Version :git ?version }" ).execSelect();
			String v = rs.next().get( "version" ).asLiteral().getString();
			return v;
		}
		catch ( Exception e )
		{
		}
		return "";
	}

	public QueryExecution createQueryExecution( String query )
	{
		String title = getMetaInfo( query ).get( "title" );
		if ( title == null )
			title = "";
		return createQueryExecution( query, title );
	}

	public QueryExecution createQueryExecution( String query, String title )
	{
		Query q = QueryFactory.create( prefix + query );

		if ( isQueryPending( query ) )
		{
			System.out.println( "PENDING: " + getMetaInfo( query ).get( "title" ) );
		}

		if ( isNative )
		{
			return QueryExecutionFactory.create( q, model );
		}
		QueryEngineHTTP engine = QueryExecutionFactory.createServiceRequest( endpoint, q );
		engine.addParam( "testid", instanceSignature );
		engine.addParam( "title", title );
		if ( !config.getSparqlEngine().equals( "" ) )
			engine.addParam( "engine", config.getSparqlEngine() );
		engine.setAllowDeflate( true );
		engine.setAllowGZip( true );
		return engine;
	}

	public boolean isNative()
	{
		return isNative;
	}

	public Model getModel()
	{
		return model;
	}

	public String getEndpoint()
	{
		return endpoint;
	}

	public String getPrefix()
	{
		return prefix;
	}
}
