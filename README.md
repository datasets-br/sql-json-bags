# sql-json-bags

A little toolkit for [multiset](https://en.wikipedia.org/wiki/Multiset) (*bag*) basic operations, storing *bags* into [JSONb](https://www.postgresql.org/docs/current/static/functions-json.html) objects.

Two flavors: [schema lib implementation](bag_as_schema.sql) and [direct implementation](bag.sql) (schema public).

## As schema bag library
As a restric jsonb-schema, with only key-value pairs and value as integer, the datatype is named *jbags*.

* **`bag.valid(JSONb)`** ret `boolean`:<br/>Validate internal representation as bag (key-multiplicity pairs).
* **`bag.sc(JSONb)`** ret `bigint`:<br/>Scalar cardinality of the bag.
* **`bag.contains(JSONb, text)`** ret  `boolean`:<br/>Checks whether the element is present (at least once) in the bag.
* **`bag.j_as_t(JSONb[])`** ret  `SETOF bag.ref`:<br/>Returns an array of JSONb-bag representations as an element-multiplicity table.
* **`bag.j_as_t(JSONb)`** ret  `SETOF bag.ref`:<br/>Similar.
* **`bag.scaled_by(JSONb,int)`** ret  `JSONb`:<br/> Scalar multiplication.  $2⊗$1.
* **`bag.intersection(JSONb[])`** ret  `JSONb`:<br/>...
* **`bag.union(JSONb[])`** ret  `JSONb`:<br/>...
* **`bag.sum(JSONb[])`** ret  `JSONb`:<br/>...
* **`bag.is_sub(JSONb, JSONb[])`** ret  `boolean`:<br/>Checks $1 ⊑ ($2[1] ∩ $2[2] ∩...), that is if $1=($1 ∩ $2[1] ∩ $2[2] ∩...).
* **`bag.intersection(JSONb,JSONb)`** ret  `JSONb`:<br/>$1 ∩ $2.
* **`bag.is_sub(JSONb, JSONb)`** ret  `boolean`:<br/> Checks $1 ⊑ $2, whether each element in the bag1 occurs in bag1 no more often than it occurs in the bag2.
* **`bag.union(JSONb,JSONb,boolean DEFAULT true)`** ret  `JSONb`:<br/>... Trying to optimize the union (or sum) of two bags.
* **`bag.j_as_t(JSONb)`** ret  `SETOF bag.ref`:<br/>

See [bag_as_schema.sql](bag_as_schema.sql) source code.

## As public library

Same as `bag.*()` but in the public catalog of functions with **`jbag` datatype as parameter**.<br/>NOTE: `j_as_t()` renamed to `astable()`.

See [bag.sql](bag.sql) source code.
