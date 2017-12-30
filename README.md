# sql-json-bags

A little toolkit for [multiset](https://en.wikipedia.org/wiki/Multiset) (*bag*) basic operations, storing *bags* into [JSONb](https://www.postgresql.org/docs/current/static/functions-json.html) objects.
As a restric jsonb-schema, with only key-value pairs and value as integer, the datatype is named *jbags*.

* **`bag.sc(jbag)`** ret `bigint`:<br/>Scalar cardinality of the bag.
* **`bag.contains(jbag, text)`** ret  `boolean`:<br/>Checks whether the element is present (at least once) in the bag.
* **`bag.j_as_t(jbag[])`** ret  `SETOF bag.ref`:<br/>Returns an array of JSONb-bag representations as an element-multiplicity table.
* **`bag.j_as_t(jbag)`** ret  `SETOF bag.ref`:<br/>Similar.
* **`bag.scaled_by(jbag,int)`** ret  `jbag`:<br/> Scalar multiplication.  $2⊗$1.
* **`bag.intersection(jbag[])`** ret  `jbag`:<br/>...
* **`bag.union(jbag[])`** ret  `jbag`:<br/>...
* **`bag.sum(jbag[])`** ret  `jbag`:<br/>...
* **`bag.is_sub(jbag, jbag[])`** ret  `boolean`:<br/>Checks $1 ⊑ ($2[1] ∩ $2[2] ∩...), that is if $1=($1 ∩ $2[1] ∩ $2[2] ∩...).
* **`bag.intersection(jbag,jbag)`** ret  `jbag`:<br/>$1 ∩ $2.
* **`bag.is_sub(jbag, jbag)`** ret  `boolean`:<br/> Checks $1 ⊑ $2, whether each element in the bag1 occurs in bag1 no more often than it occurs in the bag2.
* **`bag.union(jbag,jbag,boolean DEFAULT true)`** ret  `jbag`:<br/>... Trying to optimize the union (or sum) of two bags.
* **`bag.j_as_t(jbag)`** ret  `SETOF bag.ref`:<br/>

See [bag.sql](bag.sql) source code. 
