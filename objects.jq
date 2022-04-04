module {
    "name": "objects",
    "description": "Utility functions that pertain to objects",
    "author": "Travis C. LaGrone",
    "email": "LaGrone.T@gmail.com"
};


# TODO document rename($from; $to)
def rename($from; $to):
	."\($to)" = ."\($from)" | del(."\($from)");


# TODO document rename($mapping)
def rename($map):
    with_entries(.key as $key | .key = ($map | (."\($key)" // $key))) ;

