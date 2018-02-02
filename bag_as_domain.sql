/**
 * JSON-BAGS implemented as PUBLIC LIBRARY (jbag and jbags domains)
 * (for SCHEMA LIB implementation see bag_as_schema.sql)
 *
 * Bags are multisets. The elementes are strings and the mutiplicity an positive non-zero integer.
 * Here an implementation of basic operations like intersection, union, cardinality and multiset sum.
 *
 * See https://en.wikipedia.org/wiki/Multiset and https://doi.org/10.1080/03081078608934952
 * See typical operations at https://en.wikipedia.org/wiki/Set_(abstract_data_type)#Multiset
 */

-- PS: name union(jbag,jbag) changed to "junion" to avoid syntax error.


DROP DOMAIN IF EXISTS jbag CASCADE;
DROP DOMAIN IF EXISTS jbags CASCADE;
DROP TABLE IF EXISTS jbag_ref CASCADE;

-- -- -- -- -- -- -- -- -- -- -- -- -- --
-- -- Defining jbag and jbags datatypes
CREATE DOMAIN jbag AS JSONb
  CHECK(  VALUE IS NULL OR  jsonb_typeof(VALUE) IN ('object','null')  )
;  -- Null semantic: empty bag is JSON-null, SQL NULL is "undefined item"

CREATE or replace FUNCTION jbags_check(JSONb[]) RETURNS boolean AS $f$
    SELECT bool_and(elem is null or jsonb_typeof(elem) in ('object','null'))
    FROM unnest($1) u(elem)
$f$ language SQL IMMUTABLE;

CREATE DOMAIN jbags as JSONb[] -- no jbag[] see https://stackoverflow.com/q/48549728
    check(jbags_check(VALUE));
-- -- -- -- -- -- -- -- -- -- -- -- -- --


CREATE TABLE jbag_ref (e text, m int); -- element and its multiplicity

CREATE or replace FUNCTION valid(
  jbag
  ,p_not_emp boolean DEFAULT false
  ,p_no_digits boolean DEFAULT false
  ,p_lower boolean DEFAULT false
) RETURNS boolean AS $f$
  SELECT bool_and(    -- key condictions
    (NOT(p_not_emp) OR key!='') AND
    (NOT(p_no_digits) OR key~* '[a-z]') AND
    (NOT(p_lower) OR key=lower(key))
    AND             -- multiplicity condictions:
    jsonb_typeof(value)='number' AND vtext~'^\d+$' AND vtext::int>0
  )
  FROM (
    SELECT *, value::text as vtext, jsonb_typeof(value)
    FROM jsonb_each($1)
  ) t
$f$ language SQL IMMUTABLE;

CREATE or replace FUNCTION valid(
  jbags
  ,p_not_emp boolean DEFAULT false
  ,p_no_digits boolean DEFAULT false
  ,p_lower boolean DEFAULT false
) RETURNS boolean AS $f$
  SELECT bool_and(valid(jb)) FROM unnest($1) u(jb)
$f$ language SQL IMMUTABLE;


/**
 * Scalar cardinality of the bag.
 */
CREATE or replace FUNCTION sc(jbag) RETURNS bigint AS $f$
  SELECT SUM((value#>>'{}')::int) FROM jsonb_each($1) a
$f$ language SQL IMMUTABLE;

CREATE or replace FUNCTION scalar_cardinality(jbag) RETURNS bigint AS $wrap$ SELECT sc($1) $wrap$ language SQL IMMUTABLE;

/**
 * Checks whether the element is present (at least once) in the bag.
 */
CREATE or replace FUNCTION contains(jbag, text) RETURNS boolean AS $f$
  SELECT $1->>$2 IS NOT NULL;
$f$ language SQL IMMUTABLE;


/**
 * Returns an array of JSONb-bag representations as an element-multiplicity table.
 */
CREATE or replace FUNCTION astable(jbags) RETURNS SETOF jbag_ref AS $f$
  SELECT e, (m#>>'{}')::int
	FROM unnest($1) t(x), LATERAL jsonb_each(x) a(e,m)
$f$ language SQL IMMUTABLE;

CREATE or replace FUNCTION astable(jbag) RETURNS SETOF jbag_ref AS $f$
	-- TO DO: must handle jsonb_typeof($1)='array'
  SELECT key, (value#>>'{}')::int FROM jsonb_each($1) a
$f$ language SQL IMMUTABLE;


/**
 * Scalar multiplication.  $2⊗$1.
 */
CREATE or replace FUNCTION scaled_by(jbag,int) RETURNS jbag AS $f$
	SELECT jsonb_object_agg(e,m)::jbag
	FROM (
		SELECT e, m * $2 AS m FROM astable($1)
	) t
$f$ language SQL IMMUTABLE;


CREATE or replace FUNCTION intersection(jbags) RETURNS jbag AS $f$
	SELECT jsonb_object_agg(e,m)::jbag
	FROM (
	  SELECT e, MIN(m) AS m
		FROM astable($1)
		GROUP BY e
		HAVING COUNT(*)=array_length($1,1)
	) t
$f$ language SQL IMMUTABLE;

CREATE or replace FUNCTION junion(jbags) RETURNS jbag AS $f$
	SELECT jsonb_object_agg(e,m)::jbag
	FROM (
	  SELECT e, MAX(m) AS m
		FROM astable($1)
		GROUP BY e
	) t
$f$ language SQL IMMUTABLE;

CREATE or replace FUNCTION sum(jbags) RETURNS jbag AS $f$
SELECT jsonb_object_agg(e,m)::jbag
FROM (
  SELECT e, SUM(m) AS m
	FROM astable($1)
	GROUP BY e
) t
$f$ language SQL IMMUTABLE;


/**
 * Checks $1 ⊑ ($2[1] ∩ $2[2] ∩...), that is if $1=($1 ∩ $2[1] ∩ $2[2] ∩...)
 */
CREATE or replace FUNCTION is_sub(jbag, jbags) RETURNS boolean AS $f$
	SELECT $1 = intersection( ($2::jsonb[]||$1::jsonb)::jbags );
$f$ language SQL IMMUTABLE;

-- -- -- -- -- -- -- --
-- Optimized for binary operations, op(a,b)

/**
 * $1 ∩ $2.
 */
CREATE or replace FUNCTION intersection(jbag,jbag) RETURNS jbag AS $f$
	SELECT jsonb_object_agg( a.key, LEAST(a.value,b.value) )::jbag
	FROM  jsonb_each($1) a(key,value), jsonb_each($2) b(key,value)
	WHERE a.key=b.key
$f$ language SQL IMMUTABLE;


/**
 * Checks $1 ⊑ $2, whether each element in the bag1 occurs in bag1 no more often than it occurs in the bag2.
 */
CREATE or replace FUNCTION is_sub(jbag, jbag) RETURNS boolean AS $f$
-- compare performance with is_sub($1,array[$2])
	SELECT bool_and (CASE WHEN b.m IS NULL THEN false ELSE a.m<=b.m END)
	FROM astable($1) a LEFT JOIN  astable($2) b ON a.e=b.e
$f$ language SQL IMMUTABLE;

/**
 * ... Trying to optimize the union (or sum) of two bags.
 */
CREATE or replace FUNCTION junion(jbag,jbag,boolean DEFAULT true) RETURNS jbag AS $f$
  -- or AS $wrap$ SELECT union(array[$1,$2]) $wrap$
  SELECT jsonb_object_agg( key, v )::jbag
  FROM (
		SELECT key,  CASE WHEN $3 THEN MAX( (value#>>'{}')::int ) ELSE SUM( (value#>>'{}')::int ) END  as v
		FROM (
			(SELECT * FROM jsonb_each($1) a(key,value) )
			UNION ALL
			(SELECT * FROM jsonb_each($2) b(key,value))
		) t
		GROUP BY key
  ) t2
$f$ language SQL IMMUTABLE;
