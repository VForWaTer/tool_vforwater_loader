#!/bin/bash
# This script build a very simple markdown report with some basic information about the
# the dataset.db that was created by the tool. There are placeholders for environment 
# variables in the report template, which will be filled here

# license overview
export REPORT_LICENSE=$(/duck/duckdb -readonly -markdown /out/dataset.db "SELECT CASE WHEN list_contains(json_keys(author), 'organisation_name') THEN author->'organisation_name' ELSE author->'last_name' || ' ' || author->'first_name' END AS author, license->'short_title' as license, citation FROM metadata; ")
# metadata overview
export REPORT_META=$(/duck/duckdb -readonly -markdown /out/dataset.db "SELECT id, title FROM metadata;")

# data and aggregation overview
export REPORT_LAYERS=$(/duck/duckdb -readonly -markdown /out/dataset.db "SELECT table_name FROM information_schema.tables WHERE table_schema='main' AND table_name != 'metadata' AND table_name != 'aggregations';")
export REPORT_AGG=$(/duck/duckdb -readonly -markdown /out/dataset.db "FROM aggregations;")

# extract the first data layer name to use it as a default value for examples
export REPORT_LAYER_EXAMPLE=$(/duck/duckdb -readonly -noheader -list /out/dataset.db "SELECT table_name FROM information_schema.tables WHERE table_schema='main' AND table_name != 'metadata' AND table_name != 'aggregations' LIMIT 1;")
export REPORT_AGG_EXAMPLE=$(/duck/duckdb -readonly -noheader -list /out/dataset.db "SELECT function_name FROM aggregations WHERE aggregation_scale='temporal' LIMIT 1;")

# make a ascii plot
export REPORT_OVERVIEW_PLOT=$(/duck/duckdb -readonly -noheader -list /out/dataset.db "SELECT mean FROM  ${REPORT_AGG_EXAMPLE}('day') ORDER BY time ASC;" | gnuplot -p -e 'set terminal dumb size 90, 20; set autoscale; set ylabel "Mean"; set xlabel "DAY"; plot "/dev/stdin" using 0:1 with lines')

# write report
envsubst < /src/templates/overview.md > /out/README.md
