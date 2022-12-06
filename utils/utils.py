import collections
import json

def compare_unordered_lists(l1: [], l2: []) -> bool:
    """
    Compare two lists regardless of order of elements.
    Duplicates elements, if present, are treated as completely separate.
    IMPORTANT:  the elements of the list must match exactly,
                and *must* be HASHABLE Python entities, such as
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


def compare_recordsets(jsonable1, jsonable2) -> bool:
    r1 = [json.loads(i) for i in sorted([json.dumps(i) for i in jsonable1])]
    r2 = [json.loads(i) for i in sorted([json.dumps(i) for i in jsonable2])]
    return r1 == r2


def summarize_dataframe(df, caption="") -> None:
    """
    Show the first 5 records of the dataset, prefaced by an optional caption,
    and a list of its columns, with counts of the records in each them

    :param df:      A Pandas data frame
    :param caption: Optional string to preface.  If present, the opening statement will read
                                                 "First 5 records of <caption>:"
    :return:        None
    """
    if caption != "":
        caption = f"of `{caption}`"

    if not df.empty:
        print(f"First 5 records {caption}:")

    print(df.head(5))

    if not df.empty:
        print("Columns, with number of records in each (excluding NaN):")
        print(df.count())
    print("List of Columns: ", list(df.columns))


def simplify_dict(dct, keep_keys):
    """
    Recursively simplifies python dict by only keeping the specified keys
    :param dct:
    :param keep_keys:
    :return:
    """
    if isinstance(dct, dict):
        return {key: simplify_dict(value, keep_keys) for key, value in dct.items() if key in keep_keys}
    if isinstance(dct, list):
        return [simplify_dict(item, keep_keys) for item in dct]
    return dct


def simplify_arrows_json(raw_json):
    TO_REMOVE = ["style", "position", "caption"]
    new_json = {}
    for key, value in raw_json.items():
        if key == "style":
            continue
        elif key in ["nodes", "relationships"]:
            new_json[key] = sorted([{
                kkey: vvalue
                for kkey, vvalue in item.items() if kkey not in TO_REMOVE
            } for item in value], key=lambda x: int(x["id"].replace("n", "").replace("r", "")))
        else:
            new_json[key] = value
    return new_json
