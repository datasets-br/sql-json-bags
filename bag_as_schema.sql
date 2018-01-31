/**
 * JSON-BAGS implemented as SCHEMA LIBRARY.
 * (for PUBLIC LIB implementation see bag.sql)
 *
 * Bags are multisets. The elementes are strings and the mutiplicity an positive non-zero integer.
 * Here an implementation of basic operations like intersection, union, cardinality and multiset sum.
 *
 * See https://en.wikipedia.org/wiki/Multiset and https://doi.org/10.1080/03081078608934952
 * See typical operations at https://en.wikipedia.org/wiki/Set_(abstract_data_type)#Multiset
 */

DROP SCHEMA IF EXISTS bag CASCADE;
CREATE SCHEMA bag;

CREATE DOMAIN jbag AS JSONB
  CHECK(  VALUE IS NULL OR  jsonb_typeof(VALUE) IN ('object','null')  )
;  -- empty bag is JSON-null, and SQL NULL is "undefined"

CREATE or replace FUNCTION bag.valid(
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
  FROM (select *, value::text as vtext, jsonb_typeof(value) FROM jsonb_each($1)) t
$f$ language SQL IMMUTABLE;


CREATE TABLE bag.ref (e text, m int);

/**
 * Scalar cardinality of the bag.
 */
CREATE FUNCTION bag.sc(JSONb) RETURNS bigint AS $f$
  SELECT SUM((value#>>'{}')::int) FROM jsonb_each($1) a
$f$ language SQL IMMUTABLE;

/**
 * Checks whether the element is present (at least once) in the bag.
 */
CREATE FUNCTION bag.contains(JSONb, text) RETURNS boolean AS $f$
  SELECT $1->>$2 IS NOT NULL;
$f$ language SQL IMMUTABLE;


/**
 * Returns an array of JSONb-bag representations as an element-multiplicity table.
 */
CREATE FUNCTION bag.j_as_t(JSONb[]) RETURNS SETOF bag.ref AS $f$
  SELECT e, (m#>>'{}')::int
	FROM unnest($1) t(x), LATERAL jsonb_each(x) a(e,m)
$f$ language SQL IMMUTABLE;

CREATE FUNCTION bag.j_as_t(JSONb) RETURNS SETOF bag.ref AS $f$
	-- TO DO: must handle jsonb_typeof($1)='array'
  SELECT key, (value#>>'{}')::int FROM jsonb_each($1) a
$f$ language SQL IMMUTABLE;


/**
 * Scalar multiplication.  $2⊗$1.
 */
CREATE FUNCTION bag.scaled_by(JSONb,int) RETURNS JSONb AS $f$
	SELECT jsonb_object_agg(e,m)
	FROM (
		SELECT e, m * $2 AS m FROM bag.j_as_t($1)
	) t
$f$ language SQL IMMUTABLE;



CREATE FUNCTION bag.intersection(JSONb[]) RETURNS JSONb AS $f$
	SELECT jsonb_object_agg(e,m)
	FROM (
	  SELECT e, MIN(m) AS m
		FROM bag.j_as_t($1)
		GROUP BY e
		HAVING COUNT(*)=array_length($1,1)
	) t
$f$ language SQL IMMUTABLE;

CREATE FUNCTION bag.union(JSONb[]) RETURNS JSONb AS $f$
	SELECT jsonb_object_agg(e,m)
	FROM (
	  SELECT e, MAX(m) AS m
		FROM bag.j_as_t($1)
		GROUP BY e
	) t
$f$ language SQL IMMUTABLE;

CREATE FUNCTION bag.sum(JSONb[]) RETURNS JSONb AS $f$
SELECT jsonb_object_agg(e,m)
FROM (
  SELECT e, SUM(m) AS m
	FROM bag.j_as_t($1)
	GROUP BY e
) t
$f$ language SQL IMMUTABLE;


/**
 * Checks $1 ⊑ ($2[1] ∩ $2[2] ∩...), that is if $1=($1 ∩ $2[1] ∩ $2[2] ∩...)
 */
CREATE FUNCTION bag.is_sub(JSONb, JSONb[]) RETURNS boolean AS $f$
	SELECT $1 = bag.intersection($2||$1);
$f$ language SQL IMMUTABLE;

-- -- -- -- -- -- -- --
-- Optimized for binary operations, op(a,b)

/**
 * $1 ∩ $2.
 */
CREATE FUNCTION bag.intersection(JSONb,JSONb) RETURNS JSONb AS $f$
	SELECT jsonb_object_agg(a.key, LEAST(a.value,b.value))
	FROM  jsonb_each($1) a(key,value), jsonb_each($2) b(key,value)
	WHERE a.key=b.key
$f$ language SQL IMMUTABLE;


/**
 * Checks $1 ⊑ $2, whether each element in the bag1 occurs in bag1 no more often than it occurs in the bag2.
 */
CREATE FUNCTION bag.is_sub(JSONb, JSONb) RETURNS boolean AS $f$
-- compare performance with bag.is_sub($1,array[$2])
	SELECT bool_and (CASE WHEN b.m IS NULL THEN false ELSE a.m<=b.m END)
	FROM bag.j_as_t($1) a LEFT JOIN  bag.j_as_t($2) b ON a.e=b.e
$f$ language SQL IMMUTABLE;

/**
 * ... Trying to optimize the union (or sum) of two bags.
 */
CREATE FUNCTION bag.union(JSONb,JSONb,boolean DEFAULT true) RETURNS JSONb AS $f$
  -- or AS $wrap$ SELECT bag.union(array[$1,$2]) $wrap$
  SELECT jsonb_object_agg(key,v)
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



/* tests:
SELECT * FROM bag.j_as_t( '{"a":1,"b":3,"x":2,"z":1}'::jsonb );

SELECT * FROM bag.j_as_t(array['{"a":1,"b":3,"x":2,"z":1}'::jsonb, '{"a":2,"b":1,"k":2,"m":1}'::jsonb]);

 select bag.intersection( '{"a":1,"b":3,"x":2,"z":1}'::jsonb, '{"a":2,"b":1,"k":2,"m":1}'::jsonb  );

 select bag.union( '{"a":1,"b":3,"x":2,"z":1}'::jsonb, '{"a":2,"b":1,"k":2,"m":1}'::jsonb  );

 select * FROM bag.merge_as_table( '{"a":1,"b":3,"x":2,"z":1}'::jsonb, '{"a":2,"b":1,"k":2,"m":1}'::jsonb  ) t(k,v);

-- rev bugs
 CREATE FUNCTION bag.j_as_t(JSONb) RETURNS SETOF bag.ref AS $f$
  --Returns a JSONb-bag representation as an element-multiplicity table.
 	DECLARE
 		aux JSONb;
 		tt bag.ref;
 	BEGIN
   IF jsonb_typeof($1)='array' THEN
 		SELECT array_agg(x) INTO aux FROM (SELECT jsonb_array_elements($1)) q(x);
 		tt:= bag.j_as_t(aux);
 	ELSE
 		SELECT key, (value#>>'{}')::int INTO tt FROM jsonb_each($1) a;
 	END IF;
 	return tt;
 	END
 $f$ language PLpgSQL IMMUTABLE;

*/
