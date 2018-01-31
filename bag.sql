/**
 * See original bag_as_schema.sql comments
 */

DROP DOMAIN IF EXISTS jbag CASCADE;
CREATE DOMAIN jbag AS JSONB
  CHECK(  VALUE IS NULL OR  jsonb_typeof(VALUE) IN ('object','null')  )
;  -- empty bag is JSON-null, and SQL NULL is "undefined"

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
  FROM (select *, value::text as vtext, jsonb_typeof(value) FROM jsonb_each($1)) t
$f$ language SQL IMMUTABLE;


/**
 * Scalar cardinality of the bag.
 */
CREATE FUNCTION sc(jbag) RETURNS bigint AS $f$
  SELECT SUM((value#>>'{}')::int) FROM jsonb_each($1) a
$f$ language SQL IMMUTABLE;

CREATE FUNCTION scalar_cardinality(jbag) RETURNS bigint AS $wrap$ SELECT sc($1) $wrap$ language SQL IMMUTABLE;

/**
 * Checks whether the element is present (at least once) in the bag.
 */
CREATE FUNCTION contains(jbag, text) RETURNS boolean AS $f$
  SELECT $1->>$2 IS NOT NULL;
$f$ language SQL IMMUTABLE;


/**
 * Returns an array of JSONb-bag representations as an element-multiplicity table.
 */
CREATE FUNCTION astable(jbag[]) RETURNS SETOF jbag_ref AS $f$
  SELECT e, (m#>>'{}')::int
	FROM unnest($1) t(x), LATERAL jsonb_each(x) a(e,m)
$f$ language SQL IMMUTABLE;

CREATE FUNCTION astable(jbag) RETURNS SETOF jbag_ref AS $f$
	-- TO DO: must handle jsonb_typeof($1)='array'
  SELECT key, (value#>>'{}')::int FROM jsonb_each($1) a
$f$ language SQL IMMUTABLE;


/**
 * Scalar multiplication.  $2⊗$1.
 */
CREATE FUNCTION scaled_by(jbag,int) RETURNS jbag AS $f$
	SELECT jsonb_object_agg(e,m)
	FROM (
		SELECT e, m * $2 AS m FROM astable($1)
	) t
$f$ language SQL IMMUTABLE;



CREATE FUNCTION intersection(jbag[]) RETURNS jbag AS $f$
	SELECT jsonb_object_agg(e,m)
	FROM (
	  SELECT e, MIN(m) AS m
		FROM astable($1)
		GROUP BY e
		HAVING COUNT(*)=array_length($1,1)
	) t
$f$ language SQL IMMUTABLE;

CREATE FUNCTION union(jbag[]) RETURNS jbag AS $f$
	SELECT jsonb_object_agg(e,m)
	FROM (
	  SELECT e, MAX(m) AS m
		FROM astable($1)
		GROUP BY e
	) t
$f$ language SQL IMMUTABLE;

CREATE FUNCTION sum(jbag[]) RETURNS jbag AS $f$
SELECT jsonb_object_agg(e,m)
FROM (
  SELECT e, SUM(m) AS m
	FROM astable($1)
	GROUP BY e
) t
$f$ language SQL IMMUTABLE;


/**
 * Checks $1 ⊑ ($2[1] ∩ $2[2] ∩...), that is if $1=($1 ∩ $2[1] ∩ $2[2] ∩...)
 */
CREATE FUNCTION is_sub(jbag, jbag[]) RETURNS boolean AS $f$
	SELECT $1 = intersection($2||$1);
$f$ language SQL IMMUTABLE;

-- -- -- -- -- -- -- --
-- Optimized for binary operations, op(a,b)

/**
 * $1 ∩ $2.
 */
CREATE FUNCTION intersection(jbag,jbag) RETURNS jbag AS $f$
	SELECT jsonb_object_agg(a.key, LEAST(a.value,b.value))
	FROM  jsonb_each($1) a(key,value), jsonb_each($2) b(key,value)
	WHERE a.key=b.key
$f$ language SQL IMMUTABLE;


/**
 * Checks $1 ⊑ $2, whether each element in the bag1 occurs in bag1 no more often than it occurs in the bag2.
 */
CREATE FUNCTION is_sub(jbag, jbag) RETURNS boolean AS $f$
-- compare performance with is_sub($1,array[$2])
	SELECT bool_and (CASE WHEN b.m IS NULL THEN false ELSE a.m<=b.m END)
	FROM astable($1) a LEFT JOIN  astable($2) b ON a.e=b.e
$f$ language SQL IMMUTABLE;

/**
 * ... Trying to optimize the union (or sum) of two bags.
 */
CREATE FUNCTION union(jbag,jbag,boolean DEFAULT true) RETURNS jbag AS $f$
  -- or AS $wrap$ SELECT union(array[$1,$2]) $wrap$
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
