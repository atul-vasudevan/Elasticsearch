# Elasticsearch

From the given a corpus consisting of a set of legal case reports represented as XML documents create an index that can support fast search across such case reports. Searches can be done based on the text contained in the document as well as based on specific entity types (e.g.,
person or organization).

In the first scenario above (searching for text contained in the document) solution must allow for searching for documents containing general terms such as “criminal law” and “arrest”. In the second scenario, solution must allow for searching for documents containing specific entities (of a given type) such as organizations (e.g., “Acme Bank”) and person (e.g., “John Smith”).

In order to support queries of the second type (based on entity types), enrich the original reports with annotations that include mentions (found in the legal case reports) to persons, locations and organizations. The later will require the use of an Entity Recognition API.


The standalone Spark application must:
 Create the index (in ElasticSearch) with the corresponding mapping (as specified above)
 Perform the necessary data curation tasks to transform the input data into the representation
used for indexing (i.e., XML to JSON representation mapping), and enrich the original data
with the three entity types specified above (i.e., person, location and organization)
 Index the curated/enriched data using ElasticSearch

Download the core NLP server and get the full documentation of the tool from the link below:
https://stanfordnlp.github.io/CoreNLP/corenlp-server.html
