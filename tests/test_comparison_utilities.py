import collections


def compare_unordered_lists(l1: [], l2: []) -> bool:
    """
    Compare two lists regardless of order of elements.
    Duplicates elements, if present, are treated as completely separate.

    IMPORTANT:  the elements of the list must match exactly,
                and must be HASHABLE Python entities, such as
                strings, numbers or tuples; they can NOT be, for example, dictionaries

    Return True if the given lists match, as defined above; False, otherwise.

    EXAMPLES:   [1, 2, 3] will match [3, 2, 1]
                [] and [] will match
                ["x", (1, 2)] will match [(1, 2) , "x"] but NOT ["x", (2, 1)]
                ["a", "a"] will NOT match ["a"]

    :param l1:  A list of HASHABLE Python entities (e.g. strings or numbers)
    :param l2:  Same as above
    :return:    True if there's a match, or False otherwise
    """
    return collections.Counter(l1) == collections.Counter(l2)



def compare_recordsets(rs1: [{}], rs2: [{}]) -> bool:
    """
    We define "recordsets" as "lists of dictionaries".  Each element of the lists is regarded as a "record".

    EXAMPLE of recordset:  [{'Field_A': 1},
                            {'Field_A': 1},
                            {'Field_A': 99, 'Field_B': 'hello'}]

    Compare 2 recordsets WITHOUT REGARD to the position of the dictionaries within the lists,
    and also WITHOUT REGARD to the position of the key:value pairs within the dictionaries.
    Duplicates records, if present, are treated as completely separate.

    Return True if the given recordsets match, as defined above; False, otherwise.

    WARNING: this function is meant for comparing SMALL datasets, because it's Order n square!

    :param rs1: A (possibly empty) list of dictionaries
    :param rs2: A (possibly empty) list of dictionaries

    :return:    True if there's a match, or False otherwise
    """

    # Verify the type of the arguments
    assert isinstance(rs1, list), "compare_recordsets() : The 1st argument is not a list!  Value = " + str(rs1)
    assert isinstance(rs2, list), "compare_recordsets() : The 2nd argument is not a list!  Value = " + str(rs2)

    if len(rs1) != len(rs2):
        return False    # Datasets of different sizes will never match

    # Consider each element (i.e. a dictionary) in turn in the first list:
    #   attempt to remove it from the other list; if the removal fails, then it means
    #   that we have an element in the 1st list that is not present in the 2nd one (hence a mismatch)
    for rec1 in rs1:
        # Note: since Python 3.7 dictionaries are order-preserving, but
        #       built-in Python functions such as "remove"
        #       do not distinguish dictionaries based on order:
        #           {'a': 1, 'b': 2} will match {'b': 2, 'a': 1}

        try:
            rs2.remove(rec1)    # Remove (the first instance of) the element rec1 from the list rs2
        except Exception:
            return False        # The remove failed - i.e. the first list contains an element not in the 2nd one

    return True


def format_nodes(method_json: dict, keep_id=False):
    keys_to_remove =  ['style', 'position', 'caption'] if keep_id else  ['id', 'style', 'position', 'caption']
    nodes = []
    for i in method_json['nodes']:
        temp = {}
        for k, v in i.items():
            if (k not in keys_to_remove) and v:  # also remove empty entries e.g. properties: {}
                temp[k] = v
        nodes.append(temp)
        del temp
    return nodes


def format_relationships(method_json: dict, keep_id=False):
    keys_to_remove =  ['style', 'position', 'caption'] if keep_id else  ['id', 'style', 'position', 'caption', 'fromId', 'toId']
    relationships = []
    for i in method_json['relationships']:
        temp = {}
        for k, v in i.items():
            if (k not in keys_to_remove) and v:  # also remove empty entries e.g. properties: {}
                temp[k] = v
        relationships.append(temp)
        del temp
    return relationships


def format_json(method_json: dict, keep_id=False) -> dict:
    method_json['nodes'] = format_nodes(method_json, keep_id=keep_id)
    method_json['relationships'] = format_relationships(method_json, keep_id=keep_id)
    if 'style' in method_json:
        del method_json['style']
    return method_json


def compare_method_json(method_json, compare_method_json):
    # remove ids, style and captions from json
    method_json = format_json(method_json)
    compare_method_json = format_json(compare_method_json)

    nodes = method_json['nodes']
    compare_nodes = compare_method_json['nodes']
    rels = method_json['relationships']
    compare_rels = compare_method_json['relationships']

    # Datasets of different sizes will never match
    assert len(nodes) == len(compare_nodes)
    assert len(rels) == len(compare_rels)

    # sort keys of node and rel dicts
    sorted_nodes = [{key: sorted(value.items()) if isinstance(value, dict) else value for key, value in sorted(node.items())} for node in nodes]
    sorted_rels = [{key: sorted(value.items()) if isinstance(value, dict) else value for key, value in sorted(rel.items())} for rel in rels]
    sorted_compare_nodes = [{key: sorted(value.items()) if isinstance(value, dict) else value for key, value in sorted(node.items())} for node in compare_nodes]
    sorted_compare_rels = [{key: sorted(value.items()) if isinstance(value, dict) else value for key, value in sorted(rel.items())} for rel in compare_rels]

    # Consider each element (i.e. a dictionary) in turn in the first list:
    #   attempt to remove it from the other list; if the removal fails, then it means
    #   that we have an element in the 1st list that is not present in the 2nd one (hence a mismatch)
    for node in sorted_nodes:
        try:
            sorted_compare_nodes.remove(node)
        except Exception:
            raise KeyError(f'Could not remove node from json: {node} \n\n{sorted_nodes=} \n{sorted_compare_nodes=} \n\n{sorted_rels=} \n{sorted_compare_rels=}') # The remove failed - i.e. there is a difference in the node lists

    for rel in sorted_rels:
        try:
            sorted_compare_rels.remove(rel)
        except Exception:
            raise KeyError(f'Could not remove rel from json: {rel} \n\n{sorted_nodes=} \n{sorted_compare_nodes=} \n\n{sorted_rels=} \n{sorted_compare_rels=}') # The remove failed - i.e. there is a difference in the node lists

    return True



##########################  TESTS  ##########################


def test_compare_unordered_lists():
    # Tests for the compare_unordered_lists function

    # POSITIVE tests
    assert compare_unordered_lists([1, 2, 3] , [1, 2, 3])
    assert compare_unordered_lists([1, 2, 3] , [3, 2, 1])
    assert compare_unordered_lists([] , [])
    assert compare_unordered_lists( ["x", (1, 2)]  ,  [(1, 2) , "x"] )

    # NEGATIVE tests
    assert not compare_unordered_lists( ["x", (1, 2)]  ,  ["x", (2, 1)] )
    assert not compare_unordered_lists( ["a", "a"]  ,  ["a"] )




def test_compare_recordsets():
    # Tests for the compare_recordsets function

    # POSITIVE tests

    assert compare_recordsets(  [  {'patient_id': 444, 'gender': 'F', 'condition_id': 'happy'} ,  {'gender': 'M' , 'patient_id': 123} ],
                                [  {'patient_id': 444, 'gender': 'F', 'condition_id': 'happy'} ,  {'gender': 'M' , 'patient_id': 123} ]
                             )  # Everything absolutely identical

    assert compare_recordsets(  [  {'patient_id': 444, 'gender': 'F', 'condition_id': 'happy'} ,  {'gender': 'M' , 'patient_id': 123} ],
                                [  {'patient_id': 444, 'gender': 'F', 'condition_id': 'happy'} ,  {'patient_id': 123 , 'gender': 'M'} ]
                             )  # 2 fields reversed in last record of 2nd dataset

    assert compare_recordsets(  [  {'patient_id': 444, 'gender': 'F', 'condition_id': 'happy'} ,  {'gender': 'M' , 'patient_id': 123} ],
                                [   {'gender': 'M' , 'patient_id': 123} , {'patient_id': 444, 'gender': 'F', 'condition_id': 'happy'} ]
                             )  # Records reversed in 2nd dataset

    assert compare_recordsets(  [  {'patient_id': 444, 'gender': 'F', 'condition_id': 'happy'} ,  {'gender': 'M' , 'patient_id': 123} ],
                                [   {'patient_id': 123 , 'gender': 'M'} , {'patient_id': 444, 'gender': 'F', 'condition_id': 'happy'}  ]
                             )  # Records reversed in 2nd dataset, and fields reversed in one of them

    assert compare_recordsets(  [  {'gender': 'F', 'patient_id': 444, 'condition_id': 'happy'} ,  {'gender': 'M' , 'patient_id': 123} ],
                                [   {'patient_id': 123 , 'gender': 'M'} , {'patient_id': 444, 'condition_id': 'happy', 'gender': 'F'}  ]
                             )  # Additional order scrambling in the last test

    assert compare_recordsets([] , [])      # 2 empty datasets

    assert compare_recordsets([{'a': 1}] , [{'a': 1}])      # Minimalist data sets!

    assert compare_recordsets([{'a': 1}, {'a': 1}]  ,
                              [{'a': 1}, {'a': 1}])      # Each dataset has 2 identical records

    assert compare_recordsets([{'a': 1}, {'a': 1}, {'z': 'hello'}]  ,
                              [{'z': 'hello'}, {'a': 1}, {'a': 1}])     # Scrambled record order, with duplicates


    # NEGATIVE tests

    assert not compare_recordsets(  [  {'patient_id': 444, 'gender': 'F', 'condition_id': 'happy'} ,  {'gender': 'M' , 'patient_id': 123} ],
                                    [  {'patient_id': 444, 'gender': 'F', 'condition_id': 'happy'}  ]
                                 )  # Missing record in 2nd dataset

    assert not compare_recordsets(  [  {'patient_id': 444, 'gender': 'F', 'condition_id': 'happy'} ,  {'gender': 'M' , 'patient_id': 123} ],
                                    [  {'patient_id': 444, 'gender': 'F', 'condition_id': 'happy'} ,  {'patient_id': 123} ]
                                 ) # Missing field in last record of 2nd dataset

    assert not compare_recordsets(  [  {'patient_id': 444, 'gender': 'F', 'condition_id': 'happy'} ,  {'gender': 'M' , 'patient_id': 123} ],
                                    [  {'patient_id': 444, 'gender': 'F', 'condition_id': 'happy'} ,  {'gender': 'M' , 'patient_id': 123, 'extra_field': 'some junk'} ]
                                 ) # Extra field in 2nd dataset

    assert not compare_recordsets(  [  {'patient_id': 444, 'gender': 'F', 'condition_id': 'happy'} ,  {'gender': 'M' , 'patient_id': 123} ],
                                    [  {'patient_id': 444, 'gender': 'F', 'condition_id': 'happy'} ,  {'gender': 'M' , 'patient_id': 123} ,  {'extra_record': 'what am I doing here?'} ]
                                 )  # Extra record in 2nd dataset

    assert not compare_recordsets( [] , [{'a': 1}] )      # one empty dataset and one non-empty

    assert not compare_recordsets([{'a': 1}]  ,
                                  [{'a': 1}, {'a': 1}])    # 1 record is NOT the same things as 2 identical ones

    assert not compare_recordsets([{'a': 1}, {'a': 1}, {'z': 'hello'}]  ,
                                  [{'a': 1}, {'a': 1}])     # datasets of different size
