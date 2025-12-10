#!/bin/bash

set -e
SCRIPTDIR="$(dirname "$0")"

case "${SCRIPTDIR}" in
	/*)
		true
		;;
	*)
		SCRIPTDIR="${PWD}/${SCRIPTDIR}"
		;;
esac

# This is needed to select the right hosts
cronSubmitterConfig="${SCRIPTDIR}"/opeb-submitter/cron-submitter.ini
source "${cronSubmitterConfig}"
OPEB_HOST="${OPEB_HOST:-openebench.bsc.es}"

if [ $# -eq 3 ] ; then
	parentCacheDir="$1"
	toolsFile="$2"
	relWorkDir="$3"
	workDir="$1/$3"
	toolsFileXZ="${toolsFile}.xz"
	# This is needed to gather the logs
	exec >> "${parentCacheDir}/startlog.txt" 2>&1
	echo "[$(date -Is)] Launching pubEnricher"

	if [ ! -f "$toolsFileXZ" ] ; then
		if [ ! -f "$toolsFile" ] ; then
			mkdir -p "$(dirname "$toolsFile")"
			wget -nv -O "$toolsFile" https://${OPEB_HOST}/monitor/rest/search
		fi
		xz -9 -T 0 "${toolsFile}"
	fi
	
	# Only erase when the manifest file does not exist
	if [ ! -f "${workDir}/manifest.json" ] ; then
		rm -rf "${workDir}"
	fi
	# But not the working directory
	retval=0
	if [ ! -d "$workDir" ] ; then
		mkdir -p "$workDir"
		exec > "${workDir}/log.txt" 2>&1
		source "${SCRIPTDIR}"/.py3env/bin/activate
		set +e
		"${SCRIPTDIR}"/.py3env/bin/python "${SCRIPTDIR}"/pubEnricher.py -d -b meta -C "${SCRIPTDIR}"/cron-config.ini -D "$workDir" --use-opeb "$toolsFileXZ" "${parentCacheDir}"/pubCacheDir
		retval=$?
		set -e
		deactivate
	fi
	if [ "$retval" = 0 ] ; then
		"${SCRIPTDIR}"/opeb-submitter/result_submitter.bash "${cronSubmitterConfig}" "$workDir"
		
		# This is needed to keep a copy of the source along with the results
		unxz -c "$toolsFileXZ" > "${workDir}"/"$(basename "${toolsFile}")"
		tar -C "${parentCacheDir}" -c -p -f - "${relWorkDir}" | xz -9 -c -T0 > "${parentCacheDir}"/"$(basename "$(dirname "$0")")"-"${relWorkDir}".tar.xz
		rm -rf "${workDir}"
	else
		echo "INFO: Data submission has been suspended, as the enriching process did not finish properly" 1>&2
	fi
	
else
	echo "ERROR: This script needs three parameters: a parent workdir, the input tools file and a relative subdirectory within the parent workdir" 1>&2
fi
