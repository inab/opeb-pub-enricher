#!/bin/bash

#OPEB_HOST=openebench.bsc.es
#OPEB_METHOD=PATCH

OPEB_HOST="${OPEB_HOST:-openebench.bsc.es}"
OPEB_METHOD=POST
ALAMBIQUE_HOST="${ALAMBIQUE_HOST:-${OPEB_HOST}}"
ALAMBIQUE_METHOD=PUT

OPEB_METRICS_BASE=https://${OPEB_HOST}/monitor/metrics/
OPEB_ALAMBIQUE_BASE=https://${ALAMBIQUE_HOST}/monitor/alambique/

batchSize=1000

set -e

scriptdir="$(dirname "$0")"

case "$scriptdir" in
	/*)
		true
		;;
	*)
		scriptdir="${PWD}/${scriptdir}"
		;;
esac

eecho () {
	echo "$@" 1>&2
}

doProcessing () {
	# Classifying the input
	local -a files
	local -a dirs
	local -a discarded
	for file in "$@" ; do
		if  [ -r "$file" ] ; then
			if [ -f "$file" ] ; then
				files+=( "$file" )
			elif [ -d "$file" ] ; then
				local manif="${file}/manifest.json"
				if [ -f "${manif}" ] ; then
					for rfile in $(jq -r '.results[] | .file ' "${manif}") ; do
						local gfile
						case rfile in
							/*)
								# Do nothing (R)
								gfile="${rfile}"
								;;
							*)
								gfile="${file}/${rfile}"
								;;
						esac
						files+=( "${gfile}" )
					done	
				else
					#dirs+=( "$file" )
					eecho "Usage: $0 {config_file} {json_result_dir_with_manifest|json_files}+"
					exit 1
				fi
			else
				discarded+=( "$file" )
			fi
		else
			discarded+=( "$file" )
		fi
	done
	
	if [ ${#discarded[@]} -gt 0 ] ; then
		eecho "DISCARDED / UNREADABLE: ${#discarded[@]}"
		eecho $'\t'"${discarded[@]}"
		eecho
	fi
	
	if [ ${#files[@]} -gt 0 ] ; then
		eecho "NUM FILES TO PROCESS: ${#files[@]}"
		for pos in $(seq 0 "${batchSize}" "$((${#files[@]} - 1))") ; do
			eecho $'\t'"BATCH $pos"
			eecho
			jq --slurp --arg host "$OPEB_HOST" -f "$scriptdir"/enricher2opeb.jq "${files[@]:${pos}:${batchSize}}" | \
			curl -v -X "${OPEB_METHOD}" -u "${OPEB_METRICS_USER}":"${OPEB_METRICS_PASS}" -H 'Content-Type: application/json' \
			"${OPEB_METRICS_BASE}" -d "@-"
		done
	else
		eecho "Usage: $0 {config_file} {json_result_dir_with_manifest|json_files}+"
		exit 1
	fi
	
	# Last, the directories
	if [ ${#dirs[@]} -gt 0 ] ; then
		eecho "DIRS TO PROCESS: ${#dirs[@]}"
		eecho
		for dir in "${dirs[@]}" ; do
			eecho "PROCESSING DIR $dir"
			eecho
			
			doProcessing "$dir"/*
		done
	fi
}

if [ $# -gt 1 ]; then
	config="$1"
	shift
	
	# Loading the credentials
	source "$config"
	
	# These could have been redefined
	OPEB_HOST="${OPEB_HOST:-openebench.bsc.es}"
	OPEB_METHOD="${OPEB_METHOD:-POST}"
	ALAMBIQUE_HOST="${ALAMBIQUE_HOST:-${OPEB_HOST}}"
	ALAMBIQUE_METHOD="${ALAMBIQUE_METHOD:-PUT}"

	OPEB_METRICS_BASE="${OPEB_METRICS_BASE:-https://${OPEB_HOST}/monitor/metrics/}"
	OPEB_ALAMBIQUE_BASE="${OPEB_ALAMBIQUE_BASE:-https://${ALAMBIQUE_HOST}/monitor/alambique/}"
	
	if [ -n "$OPEB_METRICS_USER" -a -n "$OPEB_METRICS_PASS" ]; then
		doProcessing "$@"
	else
		eecho "ERROR: The config file must declare the OPEB_METRICS_USER and OPEB_METRICS_PASS variables, which are the credentials to OpenEBench"
	fi
else
	eecho "Usage: $0 {config_file} {json_result_dir_with_manifest|json_files}+"
fi
