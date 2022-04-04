module {
    "name": "sql",
    "description": "SQL-like operations (e.g. joins)",
    "author": "Travis C. LaGrone",
    "email": "LaGrone.T@gmail.com"
};


def cross_join($left_values; $right_values):
    # Performs a SQL-style cross join (i.e. cartesian product) between
    # $left_values and $right_values.
    # 
    # :param $left_values:  [ <left-value>* ]
    # :param $right_values:  [ <right-value>* ]
    # :output:  [ { "left": <left-value>, "right": <right-value> }* ]
    [
        $left_values[] as $left
        | $right_values[] as $right
        | {
            "left": $left,
            "right": $right
        }
    ] ;

def cross_join($right_values):
    # Equivalent to `. as $left_values | cross_join($left_values; $right_values)`.
    . as $left_values
    | cross_join($left_values; $right_values)
    ;


def by(index_expression):
    # :param index_expression:  <value> => <key>
    # :input:  [ <value*> ]
    # :output:  [ { "key": (<value> | index_expression), "value": <value> }* ] 
    map({
        "key": . | index_expression,
        "value": .
    }) ;


def cogroup($left_entries; $right_entries):
    # Performs an Apache Pig Latin -style cogrouping between $left_entries and
    # $right_entries on <left-entry>.key = <right-entry.key>.
    # 
    # Uses `null == null` as in jq, *not* `NULL <> NULL` as in SQL.
    # 
    # :param $left_entries:  [ { "key": <left-key>, "value": <left-value> }* ]
    # :param $right_entries:  [ { "key": <right-key>, "value": <right-value> }* ]
    # :output:  [ { "key": <key>, "left": [ <left-value>* ], "right": [ <right-value>* ] }* ]
    [
        ($left_entries | map(.side |= "left")),
        ($right_entries | map(.side |= "right"))
    ]
    | add
    | group_by(.key)
    | map({
        "key": .[0].key,
        "left": map(select(.side == "left") | .value),
        "right": map(select(.side == "right") | .value),
    }) ;

def cogroup($right_entries):
    # Equivalent to `. as $left_entries | cogroup($left_entries; $right_entries)`.
    . as $left_entries
    | cogroup($left_entries; $right_entries)
    ;


def full_join($left_entries; $right_entries):
    # Performs a SQL-style full outer join between $left_entries and $right_entries
    # on <left-entry>.key == <right-entry>.key.
    # 
    # Uses `null == null` as in jq, *not* `NULL <> NULL` as in SQL.
    # 
    # :param $left_entries:  [ { "key": <left-key>, "value": <left-value> }* ]
    # :param $right_entries:  [ { "key": <right-key>, "value": <right-value> }* ]
    # :output:
    #     [ union(
    #         { "key": <key>, "left": <left-value> },
    #         { "key": <key>, "right": <right-value> },
    #         { "key": <key>, "left": <left-value>, "right": <right-value> }
    #     )* ]
    cogroup($left_entries; $right_entries)
    | map(
        .key as $key
        | if (.left | length) == 0 then
            {
                "key": $key,
                "right": .right[]
            }
        elif (.right | length) == 0 then
            {
                "key": $key,
                "left": .left[]
            }
        else
            cross_join(.left; .right)
            | .[]
            | .key = $key
        end
    ) ;

def full_join($right_entries):
    # Equivalent to `. as $left_entries | full_join($left_entries; $right_entries)`.
    . as $left_entries
    | full_join($left_entries; $right_entries)
    ;


def left_join($left_entries; $right_entries):
    # Performs a SQL-style left outer join between $left_entries and $right_entries
    # on <left-entry>.key == <right-entry>.key.
    # 
    # Uses `null == null` as in jq, *not* `NULL <> NULL` as in SQL.
    # 
    # :param $left_entries:  [ { "key": <left-key>, "value": <left-value> }* ]
    # :param $right_entries:  [ { "key": <right-key>, "value": <right-value> }* ]
    # :output:
    #     [ union(
    #         { "key": <key>, "left": <left-value> },
    #         { "key": <key>, "left": <left-value>, "right": <right-value> }
    #     )* ]
    full_join($left_entries; $right_entries)
    | map(select(.left != null))
    ;

def left_join($right_entries):
    # Equivalent to `. as $left_entries | left_join($left_entries; $right_entries)`.
    . as $left_entries
    | left_join($left_entries; $right_entries)
    ;


def inner_join($left_entries; $right_entries):
    # Performs a SQL-style inner join between $left_entries and $right_entries
    # on <left-entry>.key == <right-entry>.key.
    # 
    # Uses `null == null` as in jq, *not* `NULL <> NULL` as in SQL.
    # 
    # :param $left_entries:  [ { "key": <left-key>, "value": <left-value> }* ]
    # :param $right_entries:  [ { "key": <right-key>, "value": <right-value> }* ]
    # :output:  [ { "key": <key>, "left": <left-value>, "right": <right-value> }* ]
    full_join($left_entries; $right_entries)
    | map(select(.left != null and .right != null))
    ;

def inner_join($right_entries):
    # Equivalent to `. as $left_entries | inner_join($left_entries; $right_entries)`.
    . as $left_entries
    | inner_join($left_entries; $right_entries)
    ;


def disjoin($left_entries; $right_entries):
    # Performs a SQL-style disjunctive outer join between $left_entries and
    # $right_entries on <left-entry>.key == <right-entry>.key.
    # 
    # Uses `null == null` as in jq, *not* `NULL <> NULL` as in SQL.
    # 
    # Analogous to the SQL query
    # ```sql
    # SELECT
    #     COALESCE(left.key, right.key) AS key,
    #     left.value AS left,
    #     right.value AS right
    # FROM left_entries AS left
    #     FULL OUTER JOIN right_entries AS right
    #         ON left.key = right.key
    # WHERE left.key IS NULL OR right.key IS NULL
    # ```
    # 
    # :param $left_entries:  [ { "key": <left-key>, "value": <left-value> }* ]
    # :param $right_entries:  [ { "key": <right-key>, "value": <right-value> }* ]
    # :output:
    #     [ union(
    #         { "key": <key>, "left": <left-value> },
    #         { "key": <key>, "right": <right-value> }
    #     )* ]
    full_join($left_entries; $right_entries)
    | map(select(
            (has("left") and has("right"))
            | not
    )) ;

def disjoin($right_entries):
    # Equivalent `. as $left_entries | outer_antijoin($left_entries; $right_entries)`.
    . as $left_entries
    | outer_antijoin($left_entries; $right_entries)
    ;


def semijoin($left_entries; $right_entries):
    # Performs a SQL-style left semi join between $left_entries and $right_entries
    # on <left-entry>.key == <right-entry>.key.
    # 
    # Uses `null == null` as in jq, *not* `NULL <> NULL` as in SQL.
    # 
    # Equivalent to the SQL query
    # ```sql
    # SELECT left.value
    # FROM left_entries AS left
    # WHERE left.key IN (
    #     SELECT right.key
    #     FROM right_entries AS right
    # )
    # ```
    # 
    # :param $left_entries:  [ { "key": <left-key>, "value": <left-value> }* ]
    # :param $right_entries:  [ { "key": <right-key>, "value": <right-value> }* ]
    # :output:  [ <left-value>* ] 
    left_join($left_entries; $right_entries)
    | map(
        select(.right != null)
        | .left
    ) ;

def semijoin($right_entries):
    # Equivalent to `. as $left_entries | semijoin($left_entries; $right_entries)`.
    . as $left_entries
    | semijoin($left_entries; $right_entries)
    ;


def antijoin($left_entries; $right_entries):
    # Performs a SQL-style left anti join between $left_entries and $right_entries
    # on <left-entry>.key == <right-entry>.key.
    # 
    # Uses `null == null` as in jq, *not* `NULL <> NULL` as in SQL.
    # 
    # Analogous to the SQL query
    # ```sql
    # SELECT left.value
    # FROM left_entries AS left
    # WHERE left.key NOT IN (
    #     SELECT right.key
    #     FROM right_entries AS right
    # )
    # ```
    # 
    # :param $left_entries:  [ { "key": <left-key>, "value": <left-value> }* ]
    # :param $right_entries:  [ { "key": <right-key>, "value": <right-value> }* ]
    # :output:  [ <left-value>* ]
    left_join($left_entries; $right_entries)
    | map(
        select(.right == null)
        | .left
    ) ;

def antijoin($right_entries):
    # Equivalent to `. as $left_entries | antijoin($left_entries; $right_entries)`.
    . as $left_entries
    | antijoin($left_entries; $right_entries)
    ;


def in_values($values):
    # Performs a SQL-style membership test (i.e. the `IN` operator) for
    # <input> in $values[]
    # 
    # Uses `null == null` as in jq, *not* `NULL <> NULL` as in SQL.
    #
    # :input:  <candidate-value>
    # :param $values:  [ <reference-value>* ]
    # :output:  <boolean>
    . as $input
    | any($values|.[]; . == $input)
    ;


def has_value($value):
    # Performs a SQL-style containment test (i.e. the `CONTAINS` operator) for
    # <input> contains $value
    # 
    # Uses `null == null` as in jq, *not* `NULL <> NULL` as in SQL.
    #
    # :input:  [ <reference-value>* ]
    # :param $value:  <candidate-value>
    # :output:  <boolean>
    any(.[]; . == $value)
    ;


# TODO document union_all(...)
def union_all($left_values; $right_values):
    $left_values + $right_values;

def union_all($right_values):
    union_all(.; $right_values);


# TODO document union(...)
def union($left_values; $right_values):
    union_all($left_values; $right_values) | unique;

def union($right_values):
    union(.; $right_values);


# TODO document intersect_all(...)
def intersect_all($left_values; $right_values):
    cogroup($left_values|by(.); $right_values|by(.))
    | map(
        select((.left | length) > 0 and (.right | length) > 0)
        | .left + .right
    )
    | add
    ;

def intersect_all($right_values):
    intersect_all(.; $right_values);


# TODO document intersect(...)
def intersect($left_values; $right_values):
    intersect_all($left_values; $right_values) | unique;

def intersect($right_values):
    intersect(.; $right_values);


# TODO document except_all(...)
def except_all($left_values; $right_values):
    antijoin($left_values|by(.); $right_values|by(.));

def except_all($right_values):
    except_all(.; $right_values);


# TODO document except(...)
def except($left_values; $right_values):
    except_all($left_values; $right_values) | unique;

def except($right_values):
    except(.; $right_values);


# TODO symmetric_difference


# TODO document index_all(...)
def index_all(index_expression):
    map({
        "key": index_expression,
        "value": .
    })
    | group_by(.key)
    | map({
        "key": first | .key,
        "value": map(.value)
    })
    | from_entries
    ;


# TODO document index(...)
def index(index_expression):
    index_all(index_expression) | map_values(last);  # QUESTION is it valid to assume that the order of elements within each group is stable relative to the input array?
