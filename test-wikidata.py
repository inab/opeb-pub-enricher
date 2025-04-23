#!/usr/bin/python3

from SPARQLWrapper import SPARQLWrapper, JSON

sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
sparql.setQuery("""
SELECT ?item ?itemLabel ?item_pubmed_id ?item_doi_id ?item_pmc_id WHERE {
  VALUES (?pubmed_id) {
    ("23514411")
  }
  VALUES (?doi_id) {
    ("10.1093/NAR/GKM298")
  }
  VALUES (?pmc_id) {
    ("2712344")
  }
  ?item wdt:P2860 ?another.
  OPTIONAL { ?item wdt:P698 ?item_pubmed_id. }
  OPTIONAL { ?item wdt:P356 ?item_doi_id. }
  OPTIONAL { ?item wdt:P932 ?item_pmc_id. }
  #?another wdt:P212 "2712344".
  { ?another wdt:P698 ?pubmed_id. }
  UNION
  { ?another wdt:P356 ?doi_id. }
  UNION
  { ?another wdt:P932 ?pmc_id. }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
}
""")
sparql.setReturnFormat(JSON)
results = sparql.query().convert()

for result in results["results"]["bindings"]:
	print("\t".join((result["item"]["value"],result["itemLabel"]["value"],result.get("item_pubmed_id",{"value":'(none)'})["value"],result.get("item_doi_id",{"value":'(none)'})["value"],result.get("item_pmc_id",{"value":'(none)'})["value"])))

