import re
from neointerface.neointerface import NeoInterface
import networkx as nx
from collections import defaultdict


def id_to_integer(node_id):
    try:
        return int("".join(char for char in node_id if char.isdigit()))
    except ValueError:
        raise ValueError(f"invalid ID: {node_id}")


def simplify_arrows_json(raw_json):
    to_remove = ["style", "position", "caption"]
    new_json = {}
    for key, value in raw_json.items():
        if key == "style":
            continue
        elif key in ["nodes", "relationships"]:
            new_json[key] = sorted([{
                kkey: vvalue
                for kkey, vvalue in item.items() if kkey not in to_remove
            } for item in value], key=lambda x: x["id"])
        else:
            new_json[key] = value
    return new_json


def visualise_json(method_json, vis_type: str = 'rigid'):

    def kamada_kawai_layout(_json):
        nx_graph = nx.Graph()
        for node in _json['nodes']:
            nx_graph.add_node(node.get('id'))
        for rel in _json['relationships']:
            nx_graph.add_edge(rel.get('fromId'), rel.get('toId'))
        return nx.kamada_kawai_layout(nx_graph)

    vis_types = ['fluid', 'rigid']
    assert vis_type in vis_types, \
        f'vis_type of {vis_type} was not a valid vis_type! Expected value in {vis_types}'
    m_count, c_count, r_count, t_count, o_count = 0, 0, 0, 0, 0

    if vis_type == 'fluid':
        base_distance = max(500, len(method_json['nodes'])*10)
        position_dict = kamada_kawai_layout(method_json)
        for node in method_json['nodes']:
            node_id = node.get('id')
            x = position_dict.get(node_id)[0]*base_distance
            y = position_dict.get(node_id)[1]*base_distance
            node['position'] = {'x': x, 'y': y}
    else:
        for count, node in enumerate(method_json['nodes']):
            base_distance = 300
            if 'core' in node['id']:
                x = 0
                y = 0
            elif 'Method' in node['labels']:
                x = m_count * base_distance
                m_count += 1
                y = base_distance
            elif 'Relationship' in node['labels']:
                x = r_count * base_distance
                r_count += 1
                y = base_distance * 2
            elif 'Class' in node['labels']:
                x = c_count * base_distance
                c_count += 1
                y = base_distance * 3
            elif 'Term' in node['labels']:
                x = t_count * base_distance
                t_count += 1
                y = base_distance * 4
            else:
                x = o_count * base_distance
                o_count += 1
                y = base_distance * 5

            node['position'] = {'x': x, 'y': y}

    for node in method_json['nodes']:
        if 'core' in node['id']:
            colour = '#68bc00'  # green
        elif 'Method' in node['labels']:
            colour = '#9f0500'  # red
        elif 'Relationship' in node['labels']:
            colour = '#009ce0'  # blue
        elif 'Class' in node['labels']:
            colour = '#000000'  # black
        elif 'Term' in node['labels']:
            colour = '#666666'  # silver
        else:
            colour = '#fa28ff'  # pink
        node['style'] = {'border-color': colour}

    for relationship in method_json['relationships']:
        if relationship:  # is not empty
            if relationship['type'] == 'METHOD_ACTION':
                colour = '#68bc00'
            elif relationship['type'] == 'NEXT':
                colour = '#9f0500'
            elif relationship['type'] == 'SOURCE_RELATIONSHIP':
                colour = '#009ce0'
            elif relationship['type'] in ['HAS_CONTROLLED_TERM', 'ON_VALUE', 'TO_VALUE', 'FROM_VALUE']:
                colour = '#666666'
            elif relationship['type'] in ['FILTER_RELATIONSHIP']:
                colour = '#dbdf00'
            else:
                colour = '#000000'
            relationship['style'] = {'arrow-color': colour}
    return method_json


def get_arrows_json_cypher(
        neo: NeoInterface,
        q: str,
        params: dict = None,
        keep_labels: list = None,
        hide_labels: list = None,
        keep_props: dict = None,
        hide_props: dict = None,
        x_shift=0,
        y_shift=0,
        limit=None,
        step=500
):
    assert limit is None or isinstance(limit, int)
    assert isinstance(step, int)
    assert (keep_labels is None) != (hide_labels is None), 'cannot use keep_labels and hide_labels together, use one or the other'
    assert (keep_props is None) != (hide_props is None), 'cannot use keep_props and hide_props together, use one or the other'
    assert step > 0
    if not params:
        params = {}
    if not hide_labels:
        hide_labels = []
    if not hide_props:
        hide_props = {}
    filter_labels = keep_labels if keep_labels else hide_labels
    filter_props = keep_props if keep_props else hide_props
    params = {**params, **{'filter_labels': filter_labels, 'filter_props': filter_props, 'x_shift': x_shift, 'y_shift': y_shift}}
    q = f"""
        //apoc.any.properties() around virtual nodes is req to get properties
        //apoc.node.labels() to get labels of virtual nodes
        //apoc.rel.startNode/endNode for virt relationships
        {q}
        WITH *
        ORDER BY id(x), id(y), id(r) 
        WITH 
            apoc.coll.toSet(collect(distinct x) + collect(distinct y)) as nds, 
            collect(distinct r) as rls  
        WITH 
        [x in nds | 
            {{		
                id: "n" + toString(id(x)),
                labels: [y in apoc.node.labels(x) WHERE {'' if keep_labels else 'NOT'} y in $filter_labels], //all but the ones in hide_labels
                properties: //all but the ones in hide_props (for each label) {{<label>: LIST OF <prop>}}
                    apoc.map.submap(
                        apoc.any.properties(x), 
                        [key in keys(apoc.any.properties(x)) WHERE {'' if keep_props else 'NOT'} key in 
                            (apoc.coll.toSet(
                                apoc.coll.flatten(
                                    [key in apoc.coll.intersection(apoc.node.labels(x), keys($filter_props)) | $filter_props[key]]
                                )
                            ))
                        ]
                    ),
                caption:"",
                style: {{}}
            }}
        ] as nds, 
        [r in [y in rls WHERE NOT y IS NULL] |
            {{
                id: "r" + toString(id(r)),
                type: type(r),
                style: {{}},
                properties: apoc.any.properties(r),
                fromId: "n" + toString(id(apoc.rel.startNode(r))),
                toId: "n" + toString(id(apoc.rel.endNode(r)))
            }}
        ] as rls
        WITH *, size(nds) as sz, 200 as step
        WITH *, toInteger(ceil(sqrt(sz))) as axlen
        WITH *, apoc.coll.flatten([x in range(1, axlen) | [y in range(1, axlen) | {{position: {{x:$x_shift+(-axlen/2+x)*step, y:$y_shift+(-axlen/2+y)*step}}}}]][0..apoc.coll.max([sz-1,1])]) as positions // grid
        //WITH *, [i in range(0, sz-1) | {{position: {{x: $x_shift+round(step*(1+toFloat(i)/10)*cos((2*pi()/10)*i)), y: $y_shift+round(step*(1+toFloat(i)/10)*sin((2*pi()/10)*i))}}}}] as positions // spiral
        WITH *, [pair in apoc.coll.zip(nds,positions) | apoc.map.merge(pair[0],pair[1])] as nds, rls
        RETURN 
            nds as nodes, 
            rls as relationships, 
            {{}} as style
        """
    res = neo.query(q, params)
    return res


def _check_on_keys_dict(node: dict, on_keys_dict: dict):
    return all(node[key] == value for key, value in on_keys_dict.items())


def _update_relationship(relationship_dict: dict, old_id, new_id):
    return {key: value if (key not in ["fromId", "toId"] or value != old_id) else new_id for key, value in
            relationship_dict.items()}


def merge_dicts_on_node_keys(left_dict: dict, right_dict: dict, on_keys_dict: dict):
    try:
        left_dict_common_node = [node for node in left_dict["nodes"] if _check_on_keys_dict(node, on_keys_dict)][0]
        right_dict_common_node = [node for node in right_dict["nodes"] if _check_on_keys_dict(node, on_keys_dict)][0]
    except IndexError:
        raise ValueError("dictionaries didn't satisfy on_keys_dict")

    output_nodes = left_dict["nodes"] + [node for node in right_dict["nodes"] if node != right_dict_common_node]
    output_relationships = left_dict["relationships"] + [
        _update_relationship(rel, right_dict_common_node["id"], left_dict_common_node["id"]) for rel in
        right_dict["relationships"]]

    result = left_dict.copy()
    result.update({"nodes": output_nodes, "relationships": output_relationships})
    return result


# This method checks if not both _id_<name> and <name> column exist in GetData and returns the missing columns
def add_warn_log_if_column_missing(col_names : list):
    missing_cols = set()
    for i in col_names:
        if i.startswith('_id_'):
            if not i[4:] in col_names:
                missing_cols.add(i[4:])
        else:
            if not "_id_"+i in col_names:
                missing_cols.add("_id_"+i)
    return missing_cols


def topological_sort(dependency_pairs):
    """Sort values subject to dependency constraints"""
    num_heads = defaultdict(int)  # num arrows pointing in
    tails = defaultdict(list)  # list of arrows going out
    for h, t in dependency_pairs:
        num_heads[t] += 1
        tails[h].append(t)

    ordered = [h for h in tails if h not in num_heads]
    for h in ordered:
        for t in tails[h]:
            num_heads[t] -= 1
            if not num_heads[t]:
                ordered.append(t)
    cyclic = [n for n, heads in num_heads.items() if heads]
    return ordered, cyclic


def extract_classes_from_query(query: str):
    pattern = r'\((\`?)(?:(?:\w|\s)+)?\1\:(.+?)\)'
    classes = sum(
        [re.sub(
            r'(`)|(\{.+?\}$)',
            '',
            x[1].replace("\n", "").strip()
        ).split(":") for x in re.findall(pattern, query, re.DOTALL)],
        []
    )
    return classes
