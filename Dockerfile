FROM maven
ENV SPARQL_ENDPOINT "http://localhost:8890/sparql"
ENV SPARQL_GRAPH "http://mu.semte.ch/application"
COPY . /app
WORKDIR /app
RUN mvn clean package assembly:single
ENTRYPOINT ["/bin/bash", "-c"]
CMD [mvn exec:java -Dexec.mainClass="com.tenforce.etms.Main"]
