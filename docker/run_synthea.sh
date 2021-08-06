#!/bin/bash
set -euo pipefail

GREEN="\e[32m"
RED="\e[31m"

PROJECT_OPT="--project"
GCS_DIR_OPT="--gcs_dir"
FHIR_LOCATION_OPT="--fhir_location"
FHIR_DATASET_OPT="--fhir_dataset"
FHIR_STORE_OPT="--fhir_store"
BQ_DATASET_OPT="--bq_dataset"

project=""
gcs_dir=""
fhir_location=""
fhir_dataset=""
fhir_store=""
bq_dataset=""
synthea_args=""

#################################################
# Prints a given message with a color.
# Arguments:
#   $1: The message
#   $2: The text for the color, e.g., "\e[32m" for green.
#################################################
color_print() {
  echo -n -e "$2"  # Sets the color to the given color.
  echo "$1"
  echo -n -e "\e[0m"  # Resets the color to no color.
}


#################################################
# Prints the usage.
#################################################
usage() {
  echo "Usage: $0  ${PROJECT_OPT} project ${GCS_DIR_OPT} gcs_dir"
  echo "       ${FHIR_LOCATION_OPT} fhir_location ${FHIR_DATASET_OPT} fhir_dataset"
  echo "       ${FHIR_STORE_OPT} fhir_store ${BQ_DATASET_OPT} bq_dataset"
  echo "       [-p 10 -s 12345 -a 30-40 -g F]"
  echo " "
  echo "  ${PROJECT_OPT} cloud project used to create GCS bucket, FHIR store, and BigQuery dataset."
  echo "  ${GCS_DIR_OPT} GCS directory to copy JSON files created by Synthea."
  echo "  ${FHIR_LOCATION_OPT} FHIR location that contains the FHIR dataset."
  echo "  ${FHIR_DATASET_OPT} FHIR dataset that contain the FHIR store."
  echo "  ${FHIR_STORE_OPT} FHIR store that will ingest the json files."
  echo "  ${BQ_DATASET_OPT} BigQuery dataset that FHIR store contect will be exported to."
  echo " "
  echo "  All remaining options will be passed to Synthea binary."
  #echo "  ${} ."
}

#################################################
# Parses arguments and does some sanity checking. Any non-recognized argument is
# added to ${TEST_ARGUMENTS} to be passed to the test script later.
# Arguments:
#   It is expected that this is called with $@ of the main script.
#################################################
parse_args() {
  while [[ $# -gt 0 ]]; do
    if [[ "$1" = "${PROJECT_OPT}" ]]; then
      shift
      if [[ $# == 0 ]]; then
        usage
        color_print "ERROR: No input provided after ${PROJECT_OPT}!" "${RED}"
        exit 1
      fi
      project="$1"
      color_print "Using GCP project: ${project}" "${GREEN}"
      shift
    elif [[ "$1" = "${GCS_DIR_OPT}" ]]; then
      shift
      if [[ $# == 0 ]]; then
        usage
        color_print "ERROR: No input provided after ${GCS_DIR_OPT}!" "${RED}"
        exit 1
      fi
      gcs_dir="$1"
      color_print "Using GCS directory: ${gcs_dir}" "${GREEN}"
      shift
    elif [[ "$1" = "${FHIR_LOCATION_OPT}" ]]; then
      shift
      if [[ $# == 0 ]]; then
        usage
        color_print "ERROR: No input provided after ${FHIR_LOCATION_OPT}!" "${RED}"
        exit 1
      fi
      fhir_location="$1"
      color_print "Using FHIR location: ${fhir_location}" "${GREEN}"
      shift
     elif [[ "$1" = "${FHIR_DATASET_OPT}" ]]; then
      shift
      if [[ $# == 0 ]]; then
        usage
        color_print "ERROR: No input provided after ${FHIR_DATASET_OPT}!" "${RED}"
        exit 1
      fi
      fhir_dataset="$1"
      color_print "Using FHIR dataset: ${fhir_dataset}" "${GREEN}"
      shift
     elif [[ "$1" = "${FHIR_STORE_OPT}" ]]; then
      shift
      if [[ $# == 0 ]]; then
        usage
        color_print "ERROR: No input provided after ${FHIR_STORE_OPT}!" "${RED}"
        exit 1
      fi
      fhir_store="$1"
      color_print "Using FHIR store: ${fhir_store}" "${GREEN}"
      shift
    elif [[ "$1" = "${BQ_DATASET_OPT}" ]]; then
      shift
      if [[ $# == 0 ]]; then
        usage
        color_print "ERROR: No input provided after ${BQ_DATASET_OPT}!" "${RED}"
        exit 1
      fi
      bq_dataset="$1"
      color_print "Using BigQuery Dataset: ${bq_dataset}" "${GREEN}"
      shift
    else
      synthea_args="${synthea_args} $1"
      shift
    fi
  done
  color_print "Args passed to synthea: ${synthea_args}" "${GREEN}"

  if [[ -z "${project}" ]]; then
    color_print "ERROR: Missing input ${PROJECT_OPT}" "${RED}"
    usage
    exit 1
  fi
  if [[ -z "${gcs_dir}" ]]; then
    color_print "ERROR: Missing input ${GCS_DIR_OPT}" "${RED}"
    exit 1
  fi
  if [[ -z "${fhir_location}" ]]; then
    color_print "ERROR: Missing input ${FHIR_LOCATION_OPT}" "${RED}"
    exit 1
  fi
  if [[ -z "${fhir_dataset}" ]]; then
    color_print "ERROR: Missing input ${FHIR_DATASET_OPT}" "${RED}"
    exit 1
  fi
  if [[ -z "${fhir_store}" ]]; then
    color_print "ERROR: Missing input ${FHIR_STORE_OPT}" "${RED}"
    exit 1
  fi
  if [[ -z "${bq_dataset}" ]]; then
    color_print "ERROR: Missing input ${BQ_DATASET_OPT}" "${RED}"
    usage
    exit 1
  fi
}

#################################################
# Main
#################################################
parse_args $@

color_print "Started creating samples using Synthea..." "${GREEN}"
java -jar synthea-with-dependencies.jar ${synthea_args}
color_print "Finished!" "${GREEN}"

color_print "Started copying JSON files to GCS bucket..." "${GREEN}"
gsutil -m cp ./output/fhir/*.json "${gcs_dir}"
color_print "Finished!" "${GREEN}"

color_print "Started importing into FHIR store..." "${GREEN}"
gcloud healthcare fhir-stores import gcs "${fhir_store}" \
  --dataset="${fhir_dataset}" \
  --location="${fhir_location}" \
  --gcs-uri="${gcs_dir}"*.json \
  --content-structure=BUNDLE_PRETTY
color_print "Finished!" "${GREEN}"

color_print "Started exporting into BigQuery..." "${GREEN}"
gcloud healthcare fhir-stores export bq "${fhir_store}" \
  --dataset="${fhir_dataset}" \
  --location="${fhir_location}" \
  --bq-dataset=bq://"${project}"."${bq_dataset}" \
  --schema-type=analytics \
  --write-disposition=write-empty
color_print "Finished!" "${GREEN}"

color_print "Started flattening tables..." "${GREEN}"
. venv3/bin/activate
python FlattenBigQuery.py "${bq_dataset}"
color_print "Finished!" "${GREEN}"

