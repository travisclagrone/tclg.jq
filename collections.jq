module {
    "name": "collections",
    "description": "Utility functions that pertain to both of the two native collection types (i.e. objects and arrays)",
    "author": "Travis C. LaGrone",
    "email": "LaGrone.T@gmail.com"
};


def swap(first_path_expression; second_path_expression):
    .
    | first_path_expression as $tmp
    | first_path_expression = second_path_expression
    | second_path_expression = $tmp
    ;
