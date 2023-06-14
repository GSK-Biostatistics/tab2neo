from abc import abstractmethod

from derivation_method.action import GetData, Link, RunScript, AssignLabel, CallAPI, BuildUri, BranchLoad, \
    BranchSave, BranchCombine, LinkStat, RunCypher


class Method:
    interface = NotImplemented

    def __init__(self):
        self.branch_dfs = None

    def retrieve_actions(self):
        actions = []
        q = """
        MATCH path=(m:Method{id:$method_id, parent_id:$parent_id})-[:METHOD_ACTION]->(action:`Method`),
        actions=(action)-[:NEXT*0..99]->(last_action:`Method`)
        WHERE NOT ( (last_action)-[:NEXT]->() ) and NOT ( ()-[:NEXT]->(action) )
        RETURN [nd in nodes(actions) | {node_id: id(nd), type: nd.type, id: nd.id}] as map
        """
        params = self.build_retrieve_actions_params()
        res = self.interface.query(q, params)
        if res:
            action_dicts = res[0]["map"]
        else:
            action_dicts = []
        for i, action_dict in enumerate(action_dicts):
            if action_dict["type"] == "filter":
                assert isinstance(actions[-1], GetData), "Filter actions can only come after GetData actions"
            elif action_dict["type"] == "get_data":
                if action_dicts[i + 1]["type"] == "filter":
                    filter_dict = action_dicts[i + 1]
                    actions.append(GetData(action_dict, filter_dict, self))
                else:
                    actions.append(GetData(action_dict, None, self))
            elif action_dict["type"] == "run_script":
                actions.append(RunScript(action_dict, self))
            elif action_dict["type"] == "call_api":
                actions.append(CallAPI(action_dict, self))
            elif action_dict["type"] == "assign_class":
                actions.append(AssignLabel(action_dict, self))
            elif action_dict["type"] == "link":
                actions.append(Link(action_dict, self))
            elif action_dict["type"] == "run_cypher":
                actions.append(RunCypher(action_dict, self))
            elif action_dict["type"] == "build_uri":
                actions.append(BuildUri(action_dict, self))
            elif action_dict["type"] == "branch_save":
                actions.append(BranchSave(action_dict, self))
            elif action_dict["type"] == "branch_load":
                actions.append(BranchLoad(action_dict, self))
            elif action_dict["type"] == "branch_combine":
                actions.append(BranchCombine(action_dict, self))
            elif action_dict["type"] == "link_stat":
                actions.append(LinkStat(action_dict, self))
            elif action_dict["type"] == "apply_stat":
                apply_stat = self.handle_supermethod(action_dict, previous_actions=actions)
                actions.append(apply_stat)
                # actions.extend(apply_stat.actions)
            elif action_dict["type"] == "decode":
                decode = self.handle_supermethod(action_dict, previous_actions=actions)
                actions.append(decode)
            elif action_dict["type"] == "subject_level_link":
                subject_level_link = self.handle_supermethod(action_dict, previous_actions=actions)
                actions.append(subject_level_link)
            elif action_dict["type"] is None:
                super_method_actions = self.handle_supermethod(action_dict)
                actions.append(super_method_actions)
            else:
                raise ValueError(f"Unknown action type {action_dict['type']}")
        return actions

    @abstractmethod
    def handle_supermethod(self, action_dict, previous_actions=None):
        pass

    @abstractmethod
    def fetch_metadata(self):
        pass

    @abstractmethod
    def build_retrieve_actions_params(self):
        pass
