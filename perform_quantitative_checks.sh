#!/bin/bash

usage() {
    echo "usage: $0 -d database_name -s schema_name -u postgres_user [-C|-D file]"
    echo "-d the database holding the osm source data (osmosis snapshot)"
    echo "-s the schema that stores the stats"
    echo "-u the postgreSQL user"
    echo "By default, the queries defined in this script are run"
    echo "and the results stored in the table \'counts\' in the database/schema"
    echo "defined in the parameters above."
    echo "Optional alternative actions:"
    echo "-C creates the stats table and schema"
    echo "-D dumps the output table to file"
    echo "Note that -C / -D must be the final option!"
    exit 0
}

create_tables() {
    SCHEMA_CREATE_QUERY="CREATE SCHEMA $SCHEMA"
    TABLE_CREATE_QUERY="CREATE TABLE $SCHEMA.counts (tstamp timestamp with time zone, counttype varchar(20), counts hstore)"
    psql -U $POSTGRES_USER -d $DB -c "$SCHEMA_CREATE_QUERY"
    psql -U $POSTGRES_USER -d $DB -c "$TABLE_CREATE_QUERY"
    exit 0
}

dump_tables() {
    pg_dump -U $POSTGRES_USER -t $SCHEMA.* $DB > $OUTFILE
    exit 0
}

while getopts "hd:s:u:CD:" option; do
    case "$option" in 
        h) usage;;
        d) DB="$OPTARG";;
        s) SCHEMA="$OPTARG";;
        u) POSTGRES_USER="$OPTARG";;
        C) create_tables;;
        D) OUTFILE="$OPTARG";dump_tables;;
        *)
            echo "Invalid option $OPTARG" 
            usage                           
        ;;
    esac
done

if [[ -z "$DB" ]] || [[ -z "$SCHEMA" ]] || [[ -z "$POSTGRES_USER" ]]; then
    echo "Error: all parameters are required!"
    usage
fi

# These are the counts to be performed. the queries all take the following form:
# insert into schema.counts
# values:
# * now() - current timestamp
# * counttype - a string of 20 chars or less representing the type of count
# * the count query, output must be a hstore. 
QUERIES=()
# this query counts the highway ways by type
QUERIES+=("insert into $SCHEMA.counts values (now(), 'highwaycount', (select hstore(array_agg(t::text),array_agg(c::text)) from (select tags->'highway' as t, count(1) as c from ways where tags?'highway' and (linestring && Box2d(st_geomfromtext('linestring(-124.7625 24.5210, -66.9326 49.3845)')) or linestring && Box2d(st_geomfromtext('linestring(-179.1506 51.2097, -129.9795 71.4410)')) or linestring && Box2d(st_geomfromtext('linestring(-160.2471 18.9117, -154.8066 22.2356)'))) group by tags->'highway') foo));")
# this query sums the highway lengths by type
QUERIES+=("insert into $SCHEMA.counts values (now(), 'highwaylength', (select hstore(array_agg(t::text),array_agg(c::text)) from (select tags->'highway' as t, sum(st_length(st_transform(linestring, 3786))) as c from ways where tags?'highway' and (linestring && Box2d(st_geomfromtext('linestring(-124.7625 24.5210, -66.9326 49.3845)')) or linestring && Box2d(st_geomfromtext('linestring(-179.1506 51.2097, -129.9795 71.4410)')) or linestring && Box2d(st_geomfromtext('linestring(-160.2471 18.9117, -154.8066 22.2356)'))) group by tags->'highway') foo));")
# this query counts the route relations by network
QUERIES+=("insert into $SCHEMA.counts values (now(), 'relationcount', (select hstore(array_agg(t::text),array_agg(c::text)) from (select tags->'network' t, count(1) c from relations where tags->'type' = 'route' and tags->'route' = 'road' and tags?'network' group by tags->'network') foo));")
# this query counts the highway nodes by type (highway=traffic_signals etc)
QUERIES+=("insert into $SCHEMA.counts values (now(), 'highwaynodecount', (select hstore(array_agg(t::text), array_agg(c::text)) from (select tags->'highway' as t, count(1) as c from nodes where tags?'highway' and (geom && Box2d(st_geomfromtext('linestring(-124.7625 24.5210, -66.9326 49.3845)')) or geom && Box2d(st_geomfromtext('linestring(-179.1506 51.2097, -129.9795 71.4410)')) or geom && Box2d(st_geomfromtext('linestring(-160.2471 18.9117, -154.8066 22.2356)'))) group by tags->'highway') foo));")
# this query counts the amenity nodes by type (pois)
QUERIES+=("insert into $SCHEMA.counts values (now(), 'amenitynodecount', (select hstore(array_agg(t::text), array_agg(c::text)) from (select tags->'amenity' as t, count(1) as c from nodes where tags?'amenity' and (geom && Box2d(st_geomfromtext('linestring(-124.7625 24.5210, -66.9326 49.3845)')) or geom && Box2d(st_geomfromtext('linestring(-179.1506 51.2097, -129.9795 71.4410)')) or geom && Box2d(st_geomfromtext('linestring(-160.2471 18.9117, -154.8066 22.2356)'))) group by tags->'amenity') foo));")
# this query counts the shop nodes by type (pois)
QUERIES+=("insert into $SCHEMA.counts values (now(), 'shopnodecount', (select hstore(array_agg(t::text), array_agg(c::text)) from (select tags->'shop' as t, count(1) as c from nodes where tags?'shop' and (geom && Box2d(st_geomfromtext('linestring(-124.7625 24.5210, -66.9326 49.3845)')) or geom && Box2d(st_geomfromtext('linestring(-179.1506 51.2097, -129.9795 71.4410)')) or geom && Box2d(st_geomfromtext('linestring(-160.2471 18.9117, -154.8066 22.2356)'))) group by tags->'shop') foo));")
echo "----------------------------------------------------------"
echo "start: `date`"
for ((q = 0; q < ${#QUERIES[@]}; q++))
do
#    echo "${QUERIES[$q]}"
    psql -U $POSTGRES_USER -d $DB -c "${QUERIES[$q]}"
done
echo "end: `date`"
echo "----------------------------------------------------------"
