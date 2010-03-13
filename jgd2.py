#!/usr/bin/env python

import sys
import os
import time
import json
try:
    import freebase
except ImportError:
    pass
from pprint import pprint as pp

class TripleException(Exception):
    pass

class IDException(Exception):
    pass

class QueryException(Exception):
    pass

class JSONLoadException(Exception):
    pass

class NullAdapter(object):
    def __init__(self, universal=False):
        pass
    def get_id(self, search_string, strict_match=False, **args):
        return None
    def filter(self, prop, ids, rhs, reverse=False):
        return (set(), {})
    def annotate(self, prop, node_id, data, reverse=False):
        return None

class FreebaseAdapter(object):
    def __init__(self, universal=False):
        if universal:
            self.universal = True
        else:
            self.universal = False
    def get_id(self, search_string, strict_match=False, **args):
        if search_string == "" or search_string is None:
            return None
        relevance = freebase.search(search_string, type=args.get(type))
        if len(relevance) == 0:
            return None
        if strict_match and len(relevance) != 1:
            return None
        return relevance[0]["id"]

    def filter(self, prop, ids, rhs, reverse=False):
        id_or_equals = set()
        if reverse:
            prop = "!%s" % prop
        for nodenum, fbid in ids:
            id_or_equals.add(fbid)
        query = {"id" : null, "id|=": list(id_or_equals)}
        query[prop] = rhs
        result = freebase.mqlread([query])
        out_set = set()
        out_data = {}
        for r in result:
            r_id = r["id"]
            for nodenum, fbid in ids:
                if fbid == r_id:
                    out_set.add(nodenum)
                    out_data[nodenum] = r[prop]

        return (out_set, out_data)

    def annotate(self, prop, node_id, data, reverse=False):
        return data


def print_timing(func):
    def wrapper(*arg):
        t1 = time.time()
        res = func(*arg)
        t2 = time.time()
        sys.stderr.write('%s took %0.3f ms\n' % (func.func_name, (t2-t1)*1000.0))
        return res
    return wrapper

class TripleStore(object):
    # These are all the indexes. 
    ids = {}
    ns_ids = {}
    predicates = {}
    literal_predicates = {}
    literals = {}
    id_printer = 0
    nodes = {}
    ns = {}
    primitive_count = 0
    __json_id = 0

    def add_to_index(self, index, l, value):
        i = index.get(l)
        if i is None:
            index[l] = [value]
        else:
            i.append(value)

    def is_id(self, id):
        if id in self.ids:
            return True
        return False

    def get_id(self, nodenum):
        return self.nodes[nodenum]["ids"][0]

    def is_ns_prop(self, prop):
        l = prop.split(":")
        if len(l) >= 2:
            if l[0] in ns:
                return True
        return False


    def __init__(self, **args):
        pass

    # Add a tuple. Triple is (subject_id, "predicate", object_id)
    def add_link(self, triple):
        s, p, o = triple
        if not s in self.ids:
            raise TripleException()
        s = self.ids[s]
        if o in self.ids:
            o_nn = self.ids[o]
            if p in self.nodes[s]["links"]:
                if o_nn in self.nodes[s]["links"][p]:
                    return
            self.add_to_index(self.nodes[o_nn]["reverse_links"], p, s)
            self.add_to_index(self.nodes[s]["links"], p, o_nn)
            self.add_to_index(self.predicates, p, (s, o_nn))
        else:
            # o is literal
            if p in self.nodes[s]["literal_links"]:
                if o in self.nodes[s]["literal_links"][p]:
                    return
            if o not in self.literals:
                self.literals[unicode(o)] = {}
            self.add_to_index(self.literals[unicode(o)], p, s)
            self.add_to_index(self.nodes[s]["literal_links"], p, unicode(o))
            self.add_to_index(self.literal_predicates, p, (s, unicode(o)))
        self.primitive_count += 1

    # Add a node. Returns the node id.
    def add_node(self, id):
        id = str(id)
        if not id.startswith(":"):
            id = ":%s" % id
        if id not in self.ids:
            self.ids[id] = self.id_printer
            self.nodes[self.id_printer] = { "ids" : [id], "links" : {}, "literal_links" : {}, "reverse_links" : {} }
            self.id_printer += 1
            self.primitive_count += 1
        return id

    def add_id_for_node(self, id, target_id):
        if not id.startswith(":"):
            id = ":%s" % id
        if id not in self.ids:
            self.ids[id] = self.ids[target_id]
            self.nodes[self.ids[target_id]]["ids"].append(id)
            return id
        else:
            raise IDException()

    # Add an adapter for a namespace
    def add_ns_adapter(self, ns, adapter_class, universal=False):
        self.ns[ns] = adapter_class(universal)
        if ns not in self.ns_ids:
            self.ns_ids[ns] = {}

    # Add data to a query result set 
    def assert_pred_on_query(self, query, pred, target):
        is_list = False
        count = 0
        if type(q) == type([]):
            is_list = True
            q = q[0]
        result_ids, result_data = self.__filter(q)
        for i in result_ids:
            tup = (self.get_id(i), pred, target)
            self.add_link(tup)
            count += 1
        return count

    def reify(self, prop, to_prop, query={}):
        if type(query) == type([]):
            query = query[0]
        out_set, out_data = self.__filter(query)
        for nn in out_set:
            if prop in self.nodes[nn]["literal_links"]:
                for x in self.nodes[nn]["literal_links"][prop]:
                    target_id = self.__generate_id()
                    newnode = self.add_node(target_id)
                    self.remove_link((self.nodes[nn]["ids"][0], prop, x))
                    self.add_link((self.nodes[nn]["ids"][0], prop, newnode))
                    self.add_link((newnode, to_prop, x))

    def generate_triples_for_id(self, id):
        #out is [forward, reverse, literal]
        out = []
        nn = self.ids[id]
        thisout = []
        for prop in self.nodes[nn]["links"]:
            for x in self.nodes[nn]["links"][prop]:
                thisout.append((id, prop, self.nodes[x]["ids"][0]))
        out.append(thisout)
        thisout = []
        for prop in self.nodes[nn]["reverse_links"]:
            for x in self.nodes[nn]["reverse_links"][prop]:
                thisout.append((self.nodes[x]["ids"][0], prop, id))
        out.append(thisout)
        thisout = []
        for prop in self.nodes[nn]["literal_links"]:
            for x in self.nodes[nn]["literal_links"][prop]:
                thisout.append((id, prop, x))
        out.append(thisout)
        return out

    def delete_node(self, id, force=False):
        triples = self.generate_triples_for_id(id)
        nn = self.ids[id]
        if not force:
            if sum(map(len, triples)) != 0:
                raise TripleException("Cannot delete linked node (use force)")
        for triple in sum(triples, []):
            self.remove_link(triple)
        for del_id in self.nodes[nn]["ids"]:
            t = self.ns_split_id(del_id)
            del self.ids[del_id]
            if t is not None:
                ns, i = t
                del self.ns_ids[ns][i]
        del self.nodes[nn]

    def remove_link(self, triple):
        s, p, o = triple
        if not s in self.ids:
            raise TripleException()
        s_nn = self.ids[s]
        if o in self.ids:
            o_nn = self.ids[o]
            # target is a link
            if p not in self.nodes[s_nn]["links"]:
                return
            if o_nn not in self.nodes[s_nn]["links"][p]:
                return
            self.nodes[s_nn]["links"][p].remove(o_nn)
            if len(self.nodes[s_nn]["links"][p]) == 0:
                del self.nodes[s_nn]["links"][p]
            self.nodes[o_nn]["reverse_links"][p].remove(s_nn)
            if len(self.nodes[o_nn]["reverse_links"][p]) == 0:
                del self.nodes[o_nn]["reverse_links"][p]
            self.predicates[p].remove((s_nn, o_nn))
            if len(self.predicates[p]) == 0:
                del self.predicates[p]
        else:
            o = unicode(o)
            # target is a literal
            if p not in self.nodes[s_nn]["literal_links"]:
                return
            if o not in self.nodes[s_nn]["literal_links"][p]:
                return
            self.nodes[s_nn]["literal_links"][p].remove(o)
            if len(self.nodes[s_nn]["literal_links"][p]) == 0:
                del self.nodes[s_nn]["literal_links"][p]
            self.literals[o][p].remove(s_nn)
            if len(self.literals[o][p]) == 0:
                del self.literals[o][p]
            if len(self.literals[o]) == 0:
                del self.literals[o]
            self.literal_predicates[p].remove((s_nn, o))
            if len(self.literal_predicates[p]) == 0:
                del self.literal_predicates[p]
        return

    def merge(self, from_id, to_id):
        if from_id not in self.ids or to_id not in self.ids:
            raise IDException()
        from_nn = self.ids[from_id]
        to_nn = self.ids[to_id]
        if from_nn == to_nn:
            return
        #Rewrite each link
        links = self.generate_triples_for_id(from_id)
        #forward
        for x in links[0]:
            s, p, o = x
            self.add_link((to_id, p, o))
        #reverse
        for x in links[1]:
            s, p, o = x
            self.add_link((s, p, to_id))
        #literal
        for x in links[2]:
            s, p, o = x
            self.add_link((to_id, p, o))
        #Gather a copy of the IDs
        ids = list(self.nodes[from_nn]["ids"])
        #Delete the from node
        self.delete_node(from_id, force=True)
        #Add the IDs
        for id in ids:
            self.add_id_for_node(id, to_id)
        return



    # Use external reconciliation based on a given property (eg, name, and using api/service/search)
    def reconcile_foreign_ids_on_prop(self, prop, ns=None, query=None, **args):
        if prop == "id":
            print "Temporarily unsupported"
            #XXX Todo: look up the id in the ids index
            return 0

        predlinks = self.literal_predicates.get(prop)

        if predlinks is None:
            print "No literal predicate by that name"
            #XXX Todo: use the id of the thing if in self.predicates
            return 0

        nslist = []
        if ns == None:
            nslist = self.ns.keys()
        elif type(ns) == type(nslist):
            nslist = ns
        else:
            nslist.append(ns)

        filterlist = None
        if query is not None:
            if type(query) == type([]):
                query = query[0]
            filterlist, data = self.__filter(query)

        n_ids = 0
        for sub, obj in predlinks:
            if filterlist is not None and sub not in filterlist:
                continue
            for namespace in nslist:
                ns_obj = self.ns[namespace]
                id_out = ns_obj.get_id(obj, **args)
                if id_out is not None:
                    self.__add_ns_id_for_node(namespace, id_out, sub)
                    n_ids += 1
        return n_ids

    def ns_split_id(self, id):
        if id.startswith(":"):
            id = id[1:]
        l = id.split(":")
        if len(l) < 2:
            return None
        return (l[0], l[1])

    def ids_in_namespace(self, ns, nums=None):
        out = []
        if nums is None:
            if self.ns[ns].universal:
                nums = self.nodes.keys()
            else:
                nums = self.ns_ids[ns].keys()
        for n in nums:
            if n not in self.ns_ids[ns]:
                if self.ns[ns].universal:
                    for key in self.nodes[n]["ids"]:
                        t = self.ns_split_id(key)
                        if t is None:
                            out.append((n, key[1:]))
                continue
            for key in self.ns_ids[ns][n]:
                ns, v = self.ns_split_id(key)
                out.append((n, v))
        return out

    def __add_ns_id_for_node(self, ns, id, nodenum):
        id = str(id)
        ns = str(ns)
        if not ns in self.ns:
            raise IDException()
        if not id.startswith(":"):
            id = ":%s" % id
        ns_id = ":%s%s" % (ns, id)
        if ns_id in self.ids:
            return ns_id
        if nodenum in self.nodes:
            self.nodes[nodenum]["ids"].append(ns_id)
            self.ids[ns_id] = nodenum
            self.add_to_index(self.ns_ids[ns], nodenum, ns_id)
            self.primitive_count += 1
        else:
            raise IDException()
        return ns_id

    def __node_id_in_ns(self, ns, nodenum):
        idlist = self.nodes[nodenum]["ids"]
        for id in idlist:
            l = id.split(":")
            if len(l) < 3:
                continue
            if l[1] == ns:
                return id
        return None

    # Serialize back to JSON
    def serialize(self):
        output = []
        def ser_sort(x, y):
            return len(self.nodes[y]["links"]) - len(self.nodes[x]["links"])
        all_nodes = self.nodes.keys()
        all_nodes.sort(ser_sort)
        done_nodes = {}

        def pre_serialize_node(s, p, o):
            rl = self.nodes[o]["reverse_links"]
            count = 0
            for p in rl:
                for l in rl[p]:
                    count += 1
            if count == 1 and o not in done_nodes:
                return serialize_node(o)
            return self.nodes[o]["ids"][0]

        def serialize_node(nn):
            this_data = {}
            for key in self.nodes[nn]["literal_links"]:
                index = self.nodes[nn]["literal_links"][key]
                d = list(index)
                if len(d) == 1:
                    d = d[0]
                this_data[key] = d

            for id in self.nodes[nn]["ids"]:
                t = self.ns_split_id(id)
                if t is None:
                    if this_data.get("id") is None:
                        this_data["id"] = id
                else:
                    ns, tid = t
                    if this_data.get("%s:id" % ns) is None:
                        this_data["%s:id" % ns] = tid

            done_nodes[nn] = True
            for key in self.nodes[nn]["links"]:
                index = self.nodes[nn]["links"][key]
                d = []
                for x in index:
                    d.append(pre_serialize_node(nn, key, x))
                if len(d) == 1:
                    d = d[0]
                this_data[key] = d

            all_nodes.remove(nn)

            return this_data

        while len(all_nodes) > 0:
            output.append(serialize_node(all_nodes[0]))
        return output



    # Here begins the hacky, hacky MQL implementation

    # The acutal entrypoint, self.MQL, is fairly easy
    # Filter by the query. Return annotated results.
    # Filter is useful in other places, annotate less so
    @print_timing
    def MQL(self, q):
        is_list = False
        if type(q) == type([]):
            is_list = True
            q = q[0]
        out_set, out_data = self.__filter(q)
        results = self.__annotate(q, out_set, out_data)
        if is_list:
            return results
        else:
            if len(results) > 0:
                return results[0]
            else:
                return None

    # For each key in the top level of the query, if the target is None, ignore it
    # (that's annotate) -- if there's a literal value, look it up, if there's another
    # list or dict as the target, recursively filter *that* set and then find what that
    # links to.
    def __filter(self, q):
        final_ids = None
        foreign_keys = False
        id_data = {}
        for pred, target in q.iteritems():
            reverse = False
            this_intermediate = set()
            if pred.startswith("!"):
                reverse = True
                pred = pred[1:]
            if self.ns_split_id(pred) is not None:
                foreign_keys = True
                continue
            if target is None:
                continue
            if type(target) == type("str") or type(target) == type(3) or type(target) == type(u"ustr"):
                target = unicode(target)
                if pred == "id":
                    if reverse:
                        raise QueryError("ID cannot be reversed")
                    if self.ids.get(target) is not None:
                        this_intermediate = set([self.ids[target]])
                    else:
                        return (set(), {})
                else:
                    if self.is_id(target):
                        index = self.nodes[self.ids[target]]["reverse_links"].get(pred)
                        if reverse:
                            index = self.nodes[self.ids[target]]["links"].get(pred)
                    else:
                        if reverse:
                            raise QueryError("Literal cannot be reversed")
                        index = self.literals.get(target)
                        if index is not None:
                            index = index.get(pred)
                    if index is None:
                        return (set(), {})
                    for id in index:
                        this_intermediate.add(id)
            if type(target) == type([]):
                target = target[0]
            if type(target) == type({}):
                if pred not in self.predicates:
                    return (set (), {})
                sub_obj = self.predicates.get(pred)
                linkage_ids, linkage_data = self.__filter(target)
                if reverse:
                    pred = "!%s" % pred
                for s, o in sub_obj:
                    if reverse:
                        o, s = s, o
                    if o in linkage_ids:
                        this_intermediate.add(s)
                        if id_data.get(s) is None:
                            id_data[s] = {}
                        if id_data[s].get(pred) is None:
                            id_data[s][pred] = {}
                        id_data[s][pred][o] = linkage_data[o]

            if final_ids == None:
                final_ids = this_intermediate
            else:
                final_ids = final_ids.intersection(this_intermediate)

            if len(final_ids) == 0:
                return (set(), {})

        # This is the block needed to call out into an adapter. Could be taken out
        # to simplify the MQL implementation
        if foreign_keys:
            for pred, target in q.iteritems():
                reverse = False
                if pred.startswith("!"):
                    reverse = True
                    pred = pred[1:]
                t = self.ns_split_id(pred)
                if t is None:
                    continue
                ns, predicate = t
                working_ids = self.ids_in_namespace(ns, final_ids)
                if predicate == "id":
                    if target == None or target == []:
                        continue
                    else:
                        if type(target) == type([]):
                            fid_list = target
                        elif type(target) == type("s") or type(target) == type(u"us"):
                            fid_list = [target]
                        else:
                            raise QueryException()
                        selected_ids = [self.ids.get(":%s:%s" % (ns, x)) for x in fid_list]
                        selected_ids = set([x for x in selected_ids if x is not None])
                        if final_ids is None:
                            final_ids = selected_ids
                        else:
                            final_ids = final_ids.intersection(selected_ids)
                    continue

                adapter = self.ns[ns]
                ids, adapter_data = adapter.filter(predicate, working_ids, target, reverse=reverse)
                if reverse:
                    pred = "!%s" % pred
                if final_ids is None:
                    final_ids = ids
                else:
                    final_ids = final_ids.intersection(ids)
                if len(final_ids) == 0:
                    return (set(), {})
                for x in final_ids:
                    if id_data.get(x) is None:
                        id_data[x] = {}
                    id_data[x][pred] = adapter_data.get(x)

        data_out = {}
        if final_ids is None:
            final_ids = set(self.nodes.keys())
        for x in final_ids:
            data_out[x] = id_data.get(x)
        return (final_ids, data_out)

    # Given a query and output from filter, fill in results and cheat where possible. 
    # If it's filtered correctly, then the literal 
    # values must be the same. The None values and dict/lists can be reinterpreted
    # recursively by looking up, for the given results' node number at this level
    def __annotate(self, q, out_set, out_data):
        output = []
        foreign_keys = False
        for x in out_set:
            data = {}
            node = self.nodes[x]
            filter_data = out_data[x]
            for pred, target in q.iteritems():
                if self.ns_split_id(pred) is not None:
                    foreign_keys = True
                    continue
                if type(target) == type("string") or type(target) == type(3) or type(target) == type(u"us"):
                    data[pred] = target
                elif type(target) == type([]):
                    if filter_data.get(pred) is None:
                        data[pred] = []
                    else:
                        data[pred] = self.__annotate(target[0], filter_data[pred].keys(), filter_data[pred])
                elif type(target) == type({}):
                    if filter_data.get(pred) is None:
                        data[pred] = None
                    else:
                        data[pred] = self.__annotate(target, filter_data[pred].keys()[0:], filter_data[pred])[0]
                elif target is None:
                    if pred == "id":
                        data[pred] = node["ids"][0]
                        continue
                    forelinks = node["links"].get(pred)
                    if forelinks is not None:
                        #ids
                        if len(forelinks) == 1:
                            data[pred] = self.nodes[forelinks[0]]["ids"][0]
                        else:
                            data[pred] = [self.nodes[x]["ids"][0] for x in forelinks]
                        continue
                    forelinks = node["literal_links"].get(pred)
                    if forelinks is not None:
                        #strs
                        if len(forelinks) == 1:
                            data[pred] = forelinks[0]
                        else:
                            data[pred] = [x for x in forelinks]
                        continue
                    data[pred] = None
                else:
                    raise QueryException()

        # This is the block needed to call out into an adapter. Could be taken out
        # to simplify the MQL implementation
            if foreign_keys:
                for pred, target in q.iteritems():
                    reverse = False
                    barepred = pred
                    if barepred.startswith("!"):
                        reverse = True
                        barepred = barepred[1:]
                    t = self.ns_split_id(barepred)
                    if type(target) == type("string") or type(target) == type(3) or type(target) == type(u"us"):
                        data[pred] = target
                        continue
                    if t is None:
                        continue
                    ns, predicate = t
                    if predicate == "id":
                        t = self.ns_ids[ns].get(x)
                        if t is None:
                            data[pred] = None
                            if target == []:
                                data[pred] = []
                        if t is not None:
                            data[pred] = [self.ns_split_id(x)[1] for x in t]
                            if target is None:
                                data[pred] = data[pred][0]
                        continue
                    data[pred] = self.ns[ns].annotate(predicate, x, filter_data[pred], reverse=reverse)

            output.append(data)
        return output

    # Here ends the hacky, hacky MQL implementation


    # Load a graph based on the results from json.load(file)
    def __generate_id(self):
        self.__json_id += 1
        return str(self.__json_id)

    @print_timing
    def load_json(self, json):
        if type(json) == type([]):
            for x in json:
                self.__json_load_helper(x)
        elif type(json) == type({}):
            self.__json_load_helper(json)

    def __json_load_helper(self, json):
        if "id" in json:
            node = self.add_node(unicode(json["id"]))
        else:
            node = self.add_node(self.__generate_id())

        for k, v in json.iteritems():
            t = self.ns_split_id(k)
            if t is not None:
                ns, pred = t
                if pred == "id":
                    #only IDs for now
                    if self.ns.get(ns) is None:
                        self.add_ns_adapter(ns, NullAdapter)
                    self.__add_ns_id_for_node(ns, v, self.ids[node])
            else:
                if type(v) == type([]):
                    for target in v:
                        if type(target) == type({}):
                            t = self.__json_load_helper(target)
                            self.add_link((node, k, t))
                        elif type(target) == type([]):
                            raise JSONLoadException()
                        else:
                            if target.startswith(":"):
                                target = self.add_node(target)
                            self.add_link((node, k, target))
                elif type(v) == type({}):
                    t = self.__json_load_helper(v)
                    self.add_link((node, k, t))
                elif v is None:
                    continue
                elif type(v) == type(3):
                    self.add_link((node, k, unicode(v)))
                else:
                    if k == "id":
                        continue
                    if v.startswith(":"):
                        v = self.add_node(v)
                    self.add_link((node, k, v))

        return node

null = None

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title="Subcommands", description="Valid subcommands", help="Additional Help", dest="subcommand")

    parser_load = subparsers.add_parser("load", help="Load a JGD file (for debugging)")
    parser_load.add_argument("file", type=argparse.FileType('r'), help="JGD file to load")
    parser_load.add_argument("-s", dest="serialize", action="store_true", default=False, help="Reserialize")

    parser_cat = subparsers.add_parser("cat", help="Join two JGD files")
    parser_cat.add_argument("file", type=argparse.FileType('r'), metavar="files", nargs="+", help="JGD files to join")

    parser_serve = subparsers.add_parser("serve", help="Start a mini-webserver with a /mqlread entrypoint")
    parser_serve.add_argument("-u", action="store_true", default=False, help="Universal -- use internal IDs as foreign IDs", dest="universal")
    parser_serve.add_argument("file", type=argparse.FileType('r'))
    parser_serve.add_argument("host", nargs="?", default="localhost")
    parser_serve.add_argument("port", type=int, default=8080)

    parser_csv = subparsers.add_parser("csv", help="Tabulate data based on properties")
    parser_csv.add_argument("--require", "-r", dest="require", default=False, action="store_true", help="Require all fields to be filled")
    parser_csv.add_argument("-q", "--query", dest="query_json", help="Query to limit results by")
    parser_csv.add_argument("file", type=argparse.FileType('r'))
    parser_csv.add_argument("columns", nargs="+")

    options = parser.parse_args()

    # Give us a blank store to start. 
    ts = TripleStore()

    if (options.subcommand == "load"):
        # Load an external json file and add the FreebaseAdapter as "fb"
        json_data = json.load(options.file)
        options.file.close()
        ts.load_json(json_data)
        if options.serialize:
            print json.dumps(ts.serialize(), indent=2)
        else:
            ts.add_ns_adapter("fb", FreebaseAdapter)

    elif (options.subcommand == "cat"):
        for x in options.file:
            json_data = json.load(x)
            x.close()
            ts.load_json(json_data)
        print json.dumps(ts.serialize(), indent=2)

    elif (options.subcommand == "serve"):
        import itty
        # Load an external json file, add the FreebaseAdapter as "fb", and
        # run the itty server to become HTTP-able
        # Use itty to make a fake mqlread entrypoint
        @itty.post("/mqlread")
        def entry_mqlread(request):
            response = {"code" : "/api/status/ok", "status" : "200 OK", "transaction_id" : "fooTID"}
            query = json.loads(request.POST.get("query"))
            pp(query)
            result = ts.MQL(query["query"])
            response["result"] = result

            return itty.Response( json.dumps(response),content_type="application/json")

        jsondata = json.load(options.file)
        options.file.close()
        ts.load_json(jsondata)
        if options.universal:
            ts.add_ns_adapter("fb", FreebaseAdapter, universal=True)
        else:
            ts.add_ns_adapter("fb", FreebaseAdapter)
        itty.run_itty(host=options.host, port=options.port)
    elif (options.subcommand == "csv"):
        import csv
        jsondata = json.load(options.file)
        options.file.close()
        ts.load_json(jsondata)
        if options.query_json:
            q = json.loads(options.query_json)
        else:
            q = [{}]
        for key in options.columns:
            q[0][key] = None
        results = ts.MQL(q)
        writer = csv.DictWriter(sys.stdout, options.columns, extrasaction="ignore")
        for r in results:
            cancel = False
            for k in options.columns:
                if r[k] is None:
                    cancel = options.require
                    continue
                r[k] = unicode(r[k]).encode("utf-8")
            if cancel:
                continue
            writer.writerow(r)

