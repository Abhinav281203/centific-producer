from typing import List, Set, Dict


def need_only(
    needs_set: Set[str], collection_list: List[Dict[str, any]]
) -> List[Dict[str, any]]:
    if len(needs_set) < 1:
        return collection_list
    return_list = []
    for collection_item in collection_list:
        # iterates through collection but chooses only k,v pairs where k is in passed 'needs' set
        list_append = {k: v for k, v in collection_item.items() if k in needs_set}
        return_list.append(list_append)
    return return_list
