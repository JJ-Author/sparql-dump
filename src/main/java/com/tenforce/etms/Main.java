package com.tenforce.etms;

import org.openrdf.model.URI;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.sparql.SPARQLRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.Rio;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

public class Main {
  public static void main(String[] args) throws RepositoryException, RDFHandlerException, FileNotFoundException {
    Logger logger = LoggerFactory.getLogger(Main.class);

    String sparqlEndpoint = System.getenv("SPARQL_ENDPOINT"); //
    String graph = System.getenv("SPARQL_GRAPH");
    String user = System.getenv("SPARQL_USER");
    String password = System.getenv("SPARQL_PASSWORD");
    String sparqlWherePart = System.getenv("SPARQL_WHERE");

    long batchSize = 50000;
    if (null != System.getenv("SPARQL_BATCHSIZE")) {
      batchSize = new Long(System.getenv("SPARQL_BATCHSIZE"));
    }

    long offset = 0;
    if (null != System.getenv("SPARQL_OFFSET")) {
      offset = new Long(System.getenv("SPARQL_OFFSET"));
    }   

    if ((null == graph && null==sparqlWherePart) || null == sparqlEndpoint) {
      logger.error("SPARQL_ENDPOINT and/or SPARQL_GRAPH or SPARQL_WHERE ENV variable are not set");
      throw new RuntimeException("SPARQL_ENDPOINT and/or SPARQL_GRAPH or SPARQL_WHERE ENV variable are not set");
    }

    File theDir = new File("dumps");
    if (!theDir.exists()) {
      theDir.mkdir();
    }

    SPARQLRepository repo = new SPARQLRepository(sparqlEndpoint);
    if (null != user && null != password) {
      logger.debug("using authentication");
      repo.setUsernameAndPassword(user, password);
    }
    repo.initialize();
    RepositoryConnection con = repo.getConnection();

    try {
      URI applicationGraph = ValueFactoryImpl.getInstance().createURI(graph);
      TupleQueryResult result = con.prepareTupleQuery(QueryLanguage.SPARQL, "SELECT (COUNT(*) as ?count) WHERE {GRAPH <" + applicationGraph.toString() + "> { ?s ?p ?o }}").evaluate();
      long amount = new Long(result.next().getBinding("count").getValue().stringValue());
      logger.info("dumping " + amount + " triples in batches of " + batchSize);
      do {
        try {
          String filename = "etms-" + offset + ".ttl";
          FileOutputStream stream = new FileOutputStream(new File(theDir.getPath(), filename));
          RDFWriter writer = Rio.createWriter(RDFFormat.NTRIPLES, stream);
          String query = "CONSTRUCT {?s ?p ?o}" +
              "WHERE { " +
              "GRAPH <" + applicationGraph.toString() + "> {" +
              "?s ?p ?o. " +
              "}}" +
              " LIMIT " + batchSize + " OFFSET " + offset;
          con.prepareGraphQuery(
              QueryLanguage.SPARQL, query).evaluate(writer);
          offset = offset + batchSize;
          logger.info("wrote  " + batchSize + " triples to " + filename);
          stream.close();
        } catch (Exception e) {
          logger.warn("failed to retrieve triples from offset" + offset + " with batchsize " + batchSize);
          logger.warn(e.getMessage());
          logger.warn(String.valueOf(e.getCause()));
        }
      } while (offset < amount);
    } catch (Exception e) {
      logger.warn(e.getMessage());
      logger.warn(String.valueOf(e.getCause()));
    } finally {
      con.close();
    }
  }
}
