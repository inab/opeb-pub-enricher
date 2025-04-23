map(
	. as {
		"@id": $id,
		"entry_pubs": $entry_pubs
	}
	|
	{
		"@id": $id | split("/") | ( .[4] |= "metrics" ) | ( .[2] |= $host ) | join("/"),
		"project.publications": (
			$entry_pubs | map(
				{
					"entries": .["found_pubs"] | map (
						{
							"title": .title?,
							"year": .year?,
							"ref_count": (.reference_count//0),
							"cit_count": (.citation_count//0),
							"refs": (.reference_stats//[]),
							"citations": (.citation_stats//[])
						}
						+
						if has("doi") then {"doi": .doi } else {} end
						+
						if has("pmid") then {"pmid": .pmid } else {} end
						+
						if has("pmcid") then {"pmcid": .pmcid } else {} end
					)
				}
			)
		)
	}
)
