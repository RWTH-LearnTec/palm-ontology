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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.ModelAndView;

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

import de.rwth.i9.palm.helper.FileHelper;
import de.rwth.i9.palm.helper.ResultSetConverter;
import de.rwth.i9.palm.model.Author;
import de.rwth.i9.palm.model.AuthorAlias;
import de.rwth.i9.palm.model.Institution;
import de.rwth.i9.palm.model.Location;
import de.rwth.i9.palm.persistence.PersistenceStrategy;

@Controller
public class SparqlController extends TripleStore
{

	@Autowired
	private PersistenceStrategy persistenceStrategy;

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

			// insertAndCheckAuthor( q, rs );

			// try to loop
			// while ( rs.hasNext() && q.startsWith(
			// "SELECT DISTINCT ?property ?hasValue ?isValueOf" ) )
			// {
			// QuerySolution qs = rs.next();
			// Resource property = qs.getResource( "property" );
			// Literal hasValue = qs.getLiteral( "hasValue" );
			// //Resource isValueOf = qs.getResource( "isValueOf"
			// ).asResource();
			//
			// //System.out.println( property.toString() + " -> " +
			// hasValue.toString() + " -> " + isValueOf.toString() );
			// System.out.println( property.toString() + " -> " +
			// hasValue.toString());
			// }
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

		return ( result.replaceAll( "[^\\x00-\\x7F]", " " ) );
	}

	private void insertAndCheckAuthor( String q, ResultSet rs )
	{
		// check for author resultset
			if ( q.contains( "http://data.linkededucation.org/resource/lak/person/" ) )
			{
				// preparing the objects
				Author author = null;
				Institution institution = null;
				Location location = null;

				while ( rs.hasNext() )
				{
					QuerySolution qs = rs.next();
					String propertyResource = qs.getResource( "property" ).toString();

					// get the institution
					if ( propertyResource.equals( "http://swrc.ontoware.org/ontology#affiliation" ) )
					{
						String affiliationUrl = qs.getResource( "hasValue" ).toString();
						String[] affiliationUrlSplit = affiliationUrl.split( "organization/" );
						if ( affiliationUrlSplit.length > 1 )
						{
							String affiliationLastUrl = affiliationUrlSplit[1];
							institution = persistenceStrategy.getInstitutionDAO().getByUri( affiliationLastUrl );

							if ( institution == null )
							{
								institution = new Institution();
								institution.setURI( affiliationLastUrl );
								institution.setName( affiliationLastUrl.replace( "-", " " ) );
								persistenceStrategy.getInstitutionDAO().persist( institution );
							}
						}
					}

					else if ( propertyResource.equals( "http://xmlns.com/foaf/0.1/based_near" ) )
					{
						String locationUrl = qs.getResource( "hasValue" ).toString();
						String[] locationUrlSplit = locationUrl.split( "resource/" );
						if ( locationUrlSplit.length > 1 )
						{
							String locationLastUrl = locationUrlSplit[1];
							location = persistenceStrategy.getLocationDAO().getByCountry( locationLastUrl );

							if ( location == null )
							{
								location = new Location();
								location.setCountry( locationLastUrl );
								persistenceStrategy.getLocationDAO().persist( location );
							}
						}
					}

					else if ( propertyResource.equals( "http://www.w3.org/2000/01/rdf-schema#label" ) )
					{
						String fullname = qs.getLiteral( "hasValue" ).toString();
						String[] fullnameArray = fullname.split( " " );
						String lastName = fullnameArray[fullnameArray.length - 1];
						String firstName = "";
						if ( fullnameArray.length - 1 > 0 )
							firstName = fullname.substring( 0, fullname.length() - lastName.length() - 1 );
						author = persistenceStrategy.getAuthorDAO().getByLastName( lastName );

						if ( author != null )
						{
							boolean isAuthorAlias = false;
							// check for author name alias, based on first
							// letter of first name
							// and institution
							if ( firstName.length() > 0 && author.getFirstName().toLowerCase().startsWith( firstName.toLowerCase().substring( 0, 1 ) ) && author.getInstitution().get( 0 ).equals( institution ) )
							{
								isAuthorAlias = true;
							}
							if ( isAuthorAlias )
							{
								if ( author.getFirstName().length() < firstName.length() )
								{
									AuthorAlias authorAlias = new AuthorAlias();
									authorAlias.setURI( author.getURI() );
									authorAlias.setName( author.getName() );

									author.addAlias( authorAlias );
									author.setName( fullname );
									author.setURI( fullname.toLowerCase().replace( ".", "" ).replace( " ", "-" ) );
									author.setFirstName( firstName );
									persistenceStrategy.getAuthorDAO().persist( author );
								}
								else
								{
									AuthorAlias authorAlias = new AuthorAlias();
									authorAlias.setURI( fullname.toLowerCase().replace( ".", "" ).replace( " ", "-" ) );
									authorAlias.setName( fullname );
									author.addAlias( authorAlias );
									persistenceStrategy.getAuthorDAO().persist( author );
								}
							}
							else
							{
								author = new Author();
								author.setName( fullname );
								author.setURI( fullname.toLowerCase().replace( ".", "" ).replace( " ", "-" ) );
								author.setFirstName( firstName );
								author.setLastName( lastName );

								if ( institution != null )
									author.addInstitution( institution );

								persistenceStrategy.getAuthorDAO().persist( author );
							}
						}
						else
						{
							author = new Author();
							author.setName( fullname );
							author.setURI( fullname.toLowerCase().replace( ".", "" ).replace( " ", "-" ) );
							author.setFirstName( firstName );
							author.setLastName( lastName );

							if ( institution != null )
								author.addInstitution( institution );

							persistenceStrategy.getAuthorDAO().persist( author );
						}
					}
				}

				if ( institution != null )
				{
					if ( institution.getLocation() == null )
					{
						institution.setLocation( location );
						persistenceStrategy.getLocationDAO().persist( location );
					}
				}

				if ( author.getBased_near() == null )
				{
					author.setBased_near( location );
					persistenceStrategy.getAuthorDAO().persist( author );
				}

			}
	}

	/**
	 * this controller send the set of sparql queries used for integration
	 * testing.
	 * 
	 * @return
	 * @throws Exception
	 */
	@RequestMapping( value = "/sparqlview/sparql/queries", method = RequestMethod.GET )
	@Transactional
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
	@Transactional
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
