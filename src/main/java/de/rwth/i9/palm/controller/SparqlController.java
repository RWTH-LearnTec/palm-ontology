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
import de.rwth.i9.palm.model.Conference;
import de.rwth.i9.palm.model.Dataset;
import de.rwth.i9.palm.model.Institution;
import de.rwth.i9.palm.model.Location;
import de.rwth.i9.palm.model.Publication;
import de.rwth.i9.palm.model.PublicationSource;
import de.rwth.i9.palm.model.Reference;
import de.rwth.i9.palm.model.Source;
import de.rwth.i9.palm.model.Subject;
import de.rwth.i9.palm.model.User;
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

			// insertReference( q, rs );

			// insertSubject( q, rs );

			// insertPublication( q, rs );

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

	// Query :
	// SELECT DISTINCT ?value
	// WHERE { ?resource <http://purl.org/dc/elements/1.1/subject> ?value }
	// ORDER BY ?value
	private void insertSubject( String q, ResultSet rs )
	{
		if ( q.contains( "http://purl.org/dc/elements/1.1/subject" ) )
		{
			while ( rs.hasNext() )
			{
				String valueResource = null;
				QuerySolution qs = rs.next();
				try
				{
					valueResource = qs.getLiteral( "value" ).toString();
					valueResource = valueResource.replaceAll( "[^\\x00-\\x7F]", " " ).trim();
					Subject subject = persistenceStrategy.getSubjectDAO().getSubjectByLabel( valueResource );

					if ( subject == null )
					{
						subject = new Subject();
						subject.setLabel( valueResource );
						persistenceStrategy.getSubjectDAO().persist( subject );
					}
				}
				catch ( Exception e )
				{
					String resource = qs.getResource( "value" ).toString();
					String[] resourceSplit = resource.split( "resource/" );
					valueResource = resourceSplit[1].toLowerCase().replace( "_", " " );
					Subject subject = persistenceStrategy.getSubjectDAO().getSubjectByLabel( valueResource );
					if ( subject != null )
					{
						subject.setResourceUri( resource );
						persistenceStrategy.getSubjectDAO().persist( subject );
					}
				}
			}
		}
	}

	private void insertPublication( String q, ResultSet rs )
	{
		if ( q.contains( "http://data.linkededucation.org/resource/lak" ) )
		{

			String confNotation = null;

			String[] split1 = q.split( "/paper" );
			String[] split2 = split1[0].split( "<" );

			if ( split2[1].contains( "conference" ) )
			{
				String[] uriSplit = split2[1].split( "conference/" );
				confNotation = uriSplit[1];
			}
			else if ( split2[1].contains( "journal" ) )
			{
				String[] uriSplit = split2[1].split( "journal/" );
				confNotation = uriSplit[1];
			}
			else
			{
				String[] uriSplit = split2[1].split( "specialissue/" );
				confNotation = uriSplit[1];
			}

			if ( confNotation.startsWith( "jedm" ) && confNotation.length() > 8 )
				confNotation = confNotation.substring( 0, 8 );

			if ( confNotation.startsWith( "jla" ) && confNotation.length() > 7 )
				confNotation = confNotation.substring( 0, 7 );

			if ( confNotation.startsWith( "jets" ) )
				confNotation = "jets20" + confNotation.substring( 4, 6 );

			Map<String, Conference> notationConferenceMaps = persistenceStrategy.getConferenceDAO().getNotationConferenceMaps();
			Conference conference = notationConferenceMaps.get( confNotation );

			System.out.println( "conf : " + conference.getConferenceGroup().getName() + " - year : " + conference.getYear() );


			Dataset dataset = persistenceStrategy.getDatasetDAO().getById( "1" );
			Source source = persistenceStrategy.getSourceDAO().getById( "1" );
			User user = persistenceStrategy.getUserDAO().getById( "1" );

			PublicationSource publicationSource = new PublicationSource();
			publicationSource.setVenue( conference.getConferenceGroup().getName() );
			publicationSource.setYear( conference.getYear() );
			publicationSource.setSource( source );
			publicationSource.setUser( user );

			Publication publication = new Publication();
			publication.setConference( conference );
			publication.setDataset( dataset );

			while ( rs.hasNext() )
			{
				QuerySolution qs = rs.next();
				String propertyResource = qs.getResource( "property" ).toString();

				if ( propertyResource.equals( "http://data.linkededucation.org/ns/linked-education.rdf#body" ) )
				{
					String contentText = qs.getLiteral( "hasValue" ).toString();
					contentText = contentText.replaceAll( "[^\\x00-\\x7F]", " " );

					publicationSource.setContentText( contentText );
					publication.setContentText( contentText );
					publication.setContentTextTokenized( contentText );
				}
				else if ( propertyResource.equals( "http://ns.nature.com/terms/hasCitation" ) )
				{
					String referenceUri = qs.getResource( "hasValue" ).toString();
					Reference reference = persistenceStrategy.getReferenceDAO().getByUri( referenceUri );

					if ( reference != null )
					{
						publication.addReference( reference );
						String pubSourceCurrentCitation = "";
						if ( publicationSource.getCitation() != null && !publicationSource.getCitation().equals( "" ) )
							pubSourceCurrentCitation = publicationSource.getCitation() + "_#_\n";
						publicationSource.setCitation( pubSourceCurrentCitation + reference.getTitle().replaceAll( "[^\\x00-\\x7F]", " " ) );
					}
				}
				else if ( propertyResource.equals( "http://purl.org/dc/elements/1.1/creator" ) )
				{
					String authorUri = qs.getResource( "hasValue" ).toString().replaceAll( "[^\\x00-\\x7F]", "" );
					String[] authorUriSplit = authorUri.split( "person/" );

					if ( authorUriSplit.length < 2 )
						continue;

					Author author = persistenceStrategy.getAuthorDAO().getByUri( authorUriSplit[1] );

					if ( author == null )
						continue;

					publication.addCoAuthor( author );

					String publicationSourceAuthor = "";
					if ( publicationSource.getAuthorString() != null && !publicationSource.getAuthorString().equals( "" ) )
						publicationSourceAuthor = publicationSource.getAuthorString() + "_#_\n";

					publicationSource.setAuthorString( publicationSourceAuthor + author.getName() );
				}
				else if ( propertyResource.equals( "http://purl.org/dc/elements/1.1/subject" ) )
				{
					try
					{
						String subjectString = qs.getLiteral( "hasValue" ).toString();
						if ( subjectString == null || subjectString.isEmpty() )
							continue;

						Subject subject = persistenceStrategy.getSubjectDAO().getSubjectByLabel( subjectString );

						if ( subject == null )
							continue;

						publication.addSubject( subject );
						String publicationSourceSubject = "";
						if ( publicationSource.getKeyword() != null && !publicationSource.getKeyword().equals( "" ) )
							publicationSourceSubject = publicationSource.getKeyword() + ", ";

						publicationSource.setKeyword( publicationSourceSubject + subject.getLabel() );
					}
					catch ( Exception e )
					{
						// nothing
					}
				}
				else if ( propertyResource.equals( "http://purl.org/dc/elements/1.1/title" ) )
				{
					String title = qs.getLiteral( "hasValue" ).toString().replaceAll( "[^\\x00-\\x7F]", "" );
					publication.setTitle( title );
					publicationSource.setTitle( title );
				}
				else if ( propertyResource.equals( "http://purl.org/ontology/bibo/abstract" ) )
				{
					String abstractText = qs.getLiteral( "hasValue" ).toString().replaceAll( "[^\\x00-\\x7F]", "" );
					publication.setAbstractText( abstractText );
					publication.setAbstractTokenized( abstractText );
					publicationSource.setAbstractText( abstractText );
				}
			}

			publication.addPublicationSource( publicationSource );
			persistenceStrategy.getPublicationDAO().persist( publication );
			
			publicationSource.setPublication( publication );
			persistenceStrategy.getPublicationSourceDAO().persist( publicationSource );
		}
	}

	private void insertReference( String q, ResultSet rs )
	{
		if ( q.contains( "http://data.linkededucation.org/resource" ) )
		{

			String[] split1 = q.split( ">" );
			String[] split2 = split1[0].split( "<" );
			String uri = split2[1];
			String title = null;
			String sameAsUri = null;

			while ( rs.hasNext() )
			{
				QuerySolution qs = rs.next();
				String propertyResource = qs.getResource( "property" ).toString();

				// get the institution
				if ( propertyResource.equals( "http://data.linkededucation.org/ns/linked-education.rdf#text" ) )
					title = qs.getLiteral( "hasValue" ).toString();
				else if ( propertyResource.equals( "http://www.w3.org/2002/07/owl#sameAs" ) )
					sameAsUri = qs.getResource( "isValueOf" ).toString();
			}

			Reference reference = new Reference();
			reference.setURI( uri );
			reference.setTitle( title );
			if ( sameAsUri != null )
				reference.setSameAsUri( sameAsUri );

			persistenceStrategy.getReferenceDAO().persist( reference );
		}

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
						if ( firstName.length() > 0 && author.getFirstName().toLowerCase().startsWith( firstName.toLowerCase().substring( 0, 1 ) ) && author.getInstitutions().get( 0 ).equals( institution ) )
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
