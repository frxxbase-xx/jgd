//warning: completely and totally untested, translated by hand from Python
var require = this.require || function(module){};
var sys = require("sys");


(function() {
    //various needed convenience functions
    function isArray(obj) {
		return Object.prototype.toString.call(obj) === "[object Array]";
    }

    /** @type {function(*):Array} */
    function toArray(val) {
        if (isArray(val)) return /**@type {Array}*/(val);
        return [val];
    }

    function getKeys(obj) {
        var keys = [];
        for (var key in obj) keys.push(key);
        return keys;
    }

    /**
      @return {string} 
    */
    function getType(v) {
        if (typeof v !== "object") return typeof v;
        if (isArray(v)) return "array";
        if (v === null) return "null";
        if (v === undefined) return "undefined";
        if (v instanceof Date) return "date";
        if (v instanceof RegExp) return "regexp";
        if (v instanceof String) return "string";
        if (v instanceof Function) return "function";
        return "object";
    }
    
    /** @constructor 
      * @param {...*} var_args the initial elements of the set
      */
    var Set = function(var_args) {
        var set = {};
        this.add = function(val) {set[val] = true;};
        this.addAll = function(array) {
            for(var i = 0; i < array.length; i++) 
                this.add(array[i]);
        }
        this.remove = function(val) {delete set[val]}
        this.contains = function(val) {return val in set;};
        //getAll only valid for 
        this.getAll = function() {var all = []; for (var val in set) all.push(val); return all;};
        this.addAll(arguments);
    }
    
    function NullAdapter(universal) {}
    NullAdapter.prototype.get_id     = function(search_string, strict_match, var_args) {return null;}
    NullAdapter.prototype.filter     = function(prop, ids, rhs, reverse) {return [new Set(), {}]}
    NullAdapter.prototype.annotate   = function(prop, node_id, data, reverse) {return null;}
    
    
    /** @constructor */
    function TripleStore() {
        //all of the indices
        this.ids = {};
        this.ns_ids = {};
        this.predicates = {};
        this.literal_predicates = {};
        this.literals = {};
        this.id_printer = 0;
        this.nodes = {};
        this.ns = {};
        this.primitive_count = 0;
        this.__json_id = 0;
    }
    
    TripleStore.prototype.add_to_index = function(index, key, value) {
        index[key] = index[key] || [];
        index[key].push(value);
    }
    
    TripleStore.prototype.is_id = function(id) {
        return id in this.ids;
    }
    
    TripleStore.prototype.get_id = function(nodenum) {
        return this.nodes[nodenum]['ids'][0]
    }
    
    TripleStore.prototype.is_ns_prop = function(prop) {
        var l = prop.split(":");
        if (l.length >= 2)
            if (l[0] in this.ns)
                return true;
        return false;
    }
    
//  Add a tuple. Triple is [subject_id, "predicate", object_id]
    TripleStore.prototype.add_link = function(triple) {
        var s = triple[0]; var p = triple[1]; var o = triple[2];
        if (!(s in this['ids']))
            throw new Error("TripleException"); //FIXME
        s = this.ids[s];
        if (o in this.ids) {
            if (p in this.nodes[s].links) {
                if (o in this.nodes[s].links[p])
                    return
                this.add_to_index(this.nodes[this.ids[o]].reverse_links, p, s);
                this.add_to_index(this.nodes[s].links, p, this.ids[o]);
                this.add_to_index(this.predicates, p, [s, this.ids[o]]);
            }
            else {
                //o is a literal
                if (p in this.nodes[s].literal_links)
                    if (o in this.nodes[s].literal_links[p])
                        return
                if (!(o in this.literals))
                    this.literals[o] = {};
                this.add_to_index(this.literals[o], p, s);
                this.add_to_index(this.nodes[s].literal_links, p, o);
                this.add_to_index(this.literal_predicates, p, [s, o]);
            }
            this.primitive_count++;
        }
    }
//  Add a node. Returns the node id.
    TripleStore.prototype.add_node = function(id) {
        id = id + "";
        if (id.charAt(0) !== ":")
            id = ":" + id;
        if (!(id in this.ids)) {
            this.ids[id] = this.id_printer;
            this.nodes[this.id_printer] = { "ids" : [id], "links" : {}, "literal_links" : {}, "reverse_links" : {} }
            this.id_printer += 1
            this.primitive_count += 1            
        }
        return id;
    }

//  Add an adapter for a namespace
    TripleStore.prototype.add_ns_adapter = function(ns, adapter_class, universal) {
        if (universal === undefined) universal = false;
        this.ns[ns] = new adapter_class(universal);
        if (!(ns in this.ns_ids))
            this.ns_ids[ns] = {};
    }

//  Add data to a query result set 
    TripleStore.prototype.assert_pred_on_query = function(query, pred, target) {
        var is_list = false;
        var count = 0;
        if (isArray(query)) {
            is_list = true;
            query = query[0];
        }
        var results = this.__filter(query);
        var result_ids = results[0]; var result_data = results[1];
        for (var i in result_ids) {
            var tup = [this.get_id(i), pred, target];
            this.add_link(tup);
            count++;            
        }
        return count
    }

    TripleStore.prototype.reify = function(prop, to_prop, query) {}
    TripleStore.prototype.iterate_triples_for_id = function(id) {}
    TripleStore.prototype.remove_link = function(triple) {
        var s = triple[0]; var p = triple[1]; var o = triple[2];
        return;
    }

    TripleStore.prototype.merge = function(from_id, to_id) {
        if (!(from_id in this.ids) || !(to_id in this.ids))
            throw new Error("IDException");
        var from_nn = this.ids[from_id];
        var to_nn = this.ids[to_id];
        if (from_nn === to_nn)
            return
        //Move each link, carefully. 
        //Move each ID
    }
    
//  Use external reconciliation based on a given property (eg, name, and using api/service/search)
    TripleStore.prototype.reconcile_foreign_ids_on_prop = function(prop, ns, query) {
        if (ns === undefined) ns = null;
        if (query === undefined) query = null;
        var args = Array.prototype.slice.call(arguments,3);

        if (prop === "id") {
            sys.puts("Temporarily unsupported")
//          XXX Todo: look up the id in the ids index
            return 0;
        }
 
        var predlinks = this.literal_predicates[prop]
 
        if (predlinks === undefined){
            sys.puts("No literal predicate by that name");
//          XXX Todo: use the id of the thing if in this.predicates
            return 0;
        }
 
        var nslist = []
        if (ns === null)
            nslist = getKeys(this.ns);
        else if (isArray(ns))
            nslist = ns
        else
            nslist.push(ns)
 
        var filterlist = null
        if (query !== null) {
            if (isArray(query))
                query = query[0]
            var result = this.__filter(query);
            filterlist = result[0]; var data = result[1];
        }

        var n_ids = 0
        for (var link in predlinks) {
            var sub = link[0]; var obj = link[1];
            if (filterlist !== null && !(sub in filterlist))
                continue
            for (var namespace in nslist) {
                var ns_obj = this.ns[namespace]
                var id_out = ns_obj.get_id.apply(ns_obj, [obj].concat(args))
                if (id_out !== null) {
                    this.__add_ns_id_for_node(namespace, id_out, sub)
                    n_ids += 1
                }
            }
        }
        return n_ids;
    }

    TripleStore.prototype.ns_split_id = function(id) {
        if (id.charAt(0) === ":")
            id = id.substring(1);
        var l = id.split(":")
        if (l.length < 2)
            return null;
        return [l[0], l[1]];
    }

    TripleStore.prototype.ids_in_namespace = function(ns, nums){
        var out = [];
        if (nums === undefined) {
            if (this.ns[ns].universal)
                nums = getKeys(this.nodes);
            else
                nums = getKeys(this.ns_ids[ns]);
        }
        for (var n in nums) {
            if (!(n in this.ns_ids[ns])) {
                if (this.ns[ns].universal) {
                    for (var key in this.nodes[n]["ids"]) {
                        var t = this.ns_split_id(key);
                        if (t === null)
                            out.push([n, key.substring(1)]);
                    }
                }
                continue;
            }
            for (var key in this.ns_ids[ns][n]) {
                var split_id = this.ns_split_id(key);
                ns = split_id[0]; var v = split_id[1];
                out.push([n, v])
            }
        }
        return out
    }

    TripleStore.prototype.__add_ns_id_for_node = function(ns, id, nodenum) {
        id = id + "";
        ns = ns + "";
        if (!(ns in this.ns))
            throw new Error("IDException");
        if (id.charAt(0) !== ":")
            id = ":" + id;
        var ns_id = ":" + ns + id;
        if (ns_id in this.ids)
            return ns_id;
        if (nodenum in this.nodes) {
            this.nodes[nodenum]["ids"].push(ns_id);
            this.ids[ns_id] = nodenum;
            this.add_to_index(this.ns_ids[ns], nodenum, ns_id);
            this.primitive_count += 1;
        }
        else
            throw new Error("IDException");
        return ns_id;
    }
 
    TripleStore.prototype.__node_id_in_ns = function(ns, nodenum) {
        var idlist = this.nodes[nodenum]["ids"]
        for (var id in idlist) {
            var l = id.split(":")
            if (l.length < 3)
                continue;
            if (l[1] == ns)
                return id;
        }
        return null;
    }
 
    // Serialize back to JSON
    TripleStore.prototype.serialize = function() {
        var output = [];
        //for use in closures
        var self = this;
        function ser_sort(x, y) {
            return self.nodes[y]["links"].length - self.nodes[x]["links"].length
        }
        var all_nodes = getKeys(this.nodes);
        all_nodes.sort(ser_sort);
        var done_nodes = {}
 
        function pre_serialize_node(s, p, o) {
            var rl = self.nodes[o]["reverse_links"];
            var count = 0;
            for (var p in rl)
                for (var l in rl[p])
                    count += 1;
            if (count === 1 && !(o in done_nodes))
                return serialize_node(o);
            return self.nodes[o]["ids"][0];
        }
        function serialize_node(nn) {
            var this_data = {};
            for (var key in self.nodes[nn]["literal_links"]) {
                var index = self.nodes[nn]["literal_links"][key];
                var d = toArray(index);
                if (d.length == 1)
                    d = d[0];
                this_data[key] = d;
            }
            
            for (var id in self.nodes[nn]["ids"]) {
                var t = self.ns_split_id(id)
                if (t === null) {
                    if (this_data["id"] === undefined)
                        this_data["id"] = id;
                }
                else {
                    var ns = t[0]; var tid = t[1];
                    if (this_data[ns+":id"] === undefined) 
                        this_data[ns+":id"] = tid;
                }
            }
            
            done_nodes[nn] = true
            for (var key in self.nodes[nn]["links"]) {
                var index = self.nodes[nn]["links"][key];
                var d = [];
                for (var x in index)
                    d.push(pre_serialize_node(nn, key, x));
                if (d.length === 1)
                    d = d[0];
                this_data[key] = d;
            }
            all_nodes.remove(nn);

            return this_data;
        }
        
        while (all_nodes.length > 0)
            output.push(serialize_node(all_nodes[0]));
        return output;
    }
    
    
 
//  Here begins the hacky, hacky MQL implementation
//  
//  The acutal entrypoint, this.MQL, is fairly easy
//  Filter by the query. Return annotated results.
//  Filter is useful in other places, annotate less so
    TripleStore.prototype.MQL = function(q) {
        var is_list = false
        if (isArray(q)) {
            is_list = true;
            q = q[0];
        }
        var filtered_out = this.__filter(q)
        var out_set = filtered_out[0]; var out_data = filtered_out[1]
        var results = this.__annotate(q, out_set, out_data);
        if (is_list)
            return results
        if (results.length > 0)
            return results[0]
        return null;
    }
 
//  For each key in the top level of the query, if the target is None, ignore it
//  (that's annotate) -- if there's a literal value, look it up, if there's another
//  list or dict as the target, recursively filter *that* set and then find what that
//  links to.
    TripleStore.prototype.__filter = function(q) {
        var final_ids = null;
        var foreign_keys = false
        var id_data = {}
        var index = null;
        for (var item in q.iteritems()) {
            var pred = item[0]; var target = item[1];
            var reverse = false
            var this_intermediate = new Set()
            if (pred.charAt(0) ==="!") {
                reverse = true;
                pred = pred.substring(1);
            }
            if (this.ns_split_id(pred) !== null) {
                foreign_keys = true
                continue;
            }
            if (target == null)
                continue;
            if (getType(target) === "string" || getType(target) === "number") {
                target = target + "";
                if (pred === "id") {
                    if (reverse)
                        throw new Error('QueryError("ID cannot be reversed")');
                    if (this.ids[target] !== undefined)
                        this_intermediate = new Set([this.ids[target]]);
                    else
                        return [new Set(), {}];
                }
                else {
                    if (this.is_id(target)) {
                        index = this.nodes[this.ids[target]]["reverse_links"][pred]
                        if (reverse)
                            index = this.nodes[this.ids[target]]["links"][pred];
                    }
                    else {
                        if (reverse)
                            throw new Error('QueryError("Literal cannot be reversed")');
                        index = this.literals[target];
                        if (index !== undefined)
                            index = index[pred];
                    }
                    if (index === undefined)
                        return [new Set(), {}];
                    for (var id in index)
                        this_intermediate.add(id);
                }
            }
            if (isArray(target))
                target = target[0];
            if (getType(target) === "object") {
                if (!(pred in this.predicates))
                    return [new Set(), {}]
                var sub_obj = this.predicates[pred];
                var filtered = this.__filter(target);
                var linkage_ids = filtered[0]; var linkage_data = filtered[1];
                if (reverse)
                    pred = "!" + pred;
                for (var s_o in sub_obj) {
                    var s = s_o[0]; var o = s_o[1];
                    if (reverse) {
                        var o_temp = s;
                        s = o;
                        o = o_temp;
                    }
                    if (o in linkage_ids) {
                        this_intermediate.add(s)
                        if (id_data[s] === undefined)
                            id_data[s] = {}
                        if (id_data[s][pred] === undefined)
                            id_data[s][pred] = {}
                        id_data[s][pred][o] = linkage_data[o]
                    }
                }
            }
            if (final_ids == null)
                final_ids = this_intermediate;
            else
                final_ids = final_ids.intersection(this_intermediate);
 
            if (final_ids.length == 0)
                return [new Set(), {}]
        }
        // This is the block needed to call out into an adapter. Could be taken out
        // to simplify the MQL implementation
        if (foreign_keys) {
            for (var pred_target in q.iteritems()) {
                var pred = pred_target[0]; var target = pred_target[1];
                reverse = false
                if (pred.startswith("!")) {
                    reverse = true
                    pred = pred.substring(1);
                }
                var t = this.ns_split_id(pred)
                if (t === null)
                    continue
                var ns = t[0]; var predicate = t[1];
                var working_ids = this.ids_in_namespace(ns, final_ids)
                if (predicate == "id") {
                    if (target == null || (isArray(target) && target.length === 0))
                        continue
                    else {
                        var fid_list;
                        if (isArray(target))
                            fid_list = target
                        else if (getType(target) === "string")
                            fid_list = [target]
                        else
                            throw new Error('QueryException()');
                        var selected_ids = new Set();
                        for (var x in fid_list)
                            selected_ids.add(this.ids[":" + ns + ":" + x]);
                        // how could x ever be None here? [x for x in selected_ids if x is not None]
                        if (final_ids == null)
                            final_ids = selected_ids
                        else
                            final_ids = final_ids.intersection(selected_ids)
                    }
                    continue;
                }

                var adapter = this.ns[ns]
                var ids_data = adapter.filter(predicate, working_ids, target, reverse=reverse);
                var ids = ids_data[0]; var adapter_data = ids_data[1];
                if (reverse)
                    pred = "!" + pred;
                if (final_ids == null)
                    final_ids = ids;
                else
                    final_ids = final_ids.intersection(ids);
                if (final_ids.length == 0)
                    return (new Set(), {})
                for (var x in final_ids) {
                    if (id_data[x] === undefined)
                        id_data[x] = {}
                    id_data[x][pred] = adapter_data[x];
                }
            }
        }
        
        var data_out = {};
        if (final_ids == null)
            final_ids = new Set(getKeys(this.nodes));
        for (var x in final_ids)
            data_out[x] = id_data[x];
        return [final_ids, data_out];
    }

    // Given a query and output from filter, fill in results and cheat where possible. 
    // If it's filtered correctly, then the literal 
    // values must be the same. The None values and dict/lists can be reinterpreted
    // recursively by looking up, for the given results' node number at this level
    TripleStore.prototype.__annotate = function(q, out_set, out_data) {
        var output = []
        var foreign_keys = false
        for (var x in out_set) {
            var data = {}
            var node = this.nodes[x]
            var filter_data = out_data[x]
            for (var pred_target in q.iteritems()){
                var pred = pred_target[0]; var target = pred_target[1];
                if (this.ns_split_id(pred) != null) {
                    foreign_keys = true;
                    continue;
                }
                if (getType(target) === "string" || getType(target) === "number")
                    data[pred] = target
                else if (isArray(target)) {
                    if (filter_data[pred] == null)
                        data[pred] = []
                    else
                        data[pred] = this.__annotate(target[0], getKeys(filter_data[pred]), filter_data[pred])
                }
                else if (getType(target) == "object"){
                    if (filter_data[pred] == null)
                        data[pred] = null
                    else
                        data[pred] = this.__annotate(target, getKeys(filter_data[pred]), filter_data[pred])[0]
                }
                else if (target == null) {
                    if (pred == "id") {
                        data[pred] = node["ids"][0];
                        continue;
                    }
                    var forelinks = node["links"][pred];
                    if (forelinks != null){
                        //ids
                        if (forelinks.length == 1)
                            data[pred] = this.nodes[forelinks[0]]["ids"][0];
                        else {
                            var temp_array = [];
                            for (var x in forelinks)
                                temp_array.push(this.nodes[x]['ids'][0])
                            data[pred] = temp_array;
                        }
                        continue;
                    }
                    forelinks = node["literal_links"][pred];
                    if (forelinks != null) {
                        //strs
                        if (forelinks.length == 1)
                            data[pred] = forelinks[0]
                        else
                            data[pred] = Array.prototype.slice.call(forelinks);
                        continue;
                    }
                    data[pred] = null;
                }
                else
                    throw new Error('QueryException()');
            }
            
        // This is the block needed to call out into an adapter. Could be taken out
        // to simplify the MQL implementation
            if (foreign_keys) {
                for (var pred_target in q.iteritems()) {
                    var pred = pred_target[0]; var target = pred_target[1];
                    var reverse = false;
                    var barepred = pred;
                    if (barepred.charAt(0) === "!") {
                        reverse = true;
                        barepred = barepred.substring(1);
                    }
                    var t = this.ns_split_id(barepred);
                    if (getType(target) === "string" || getType(target) === 'number') {
                        data[pred] = target;
                        continue;
                    }
                    if (t === null)
                        continue;
                    var ns = t[0]; var predicate = t[1];
                    if (predicate == "id") {
                        t = this.ns_ids[ns][x]
                        if (t == null) {
                            data[pred] = null
                            if (target == [])
                                data[pred] = [];
                        }
                        if (t != null) {
                            var temp_array = [];
                            for (var x in t)
                                temp_array.push(this.ns_split_id(x)[1]);
                            data[pred] = temp_array;
                            if (target == null)
                                data[pred] = data[pred][0]
                        }
                        continue;
                    }
                    data[pred] = this.ns[ns].annotate(predicate, x, filter_data[pred], reverse=reverse)
                }
            }
            output.push(data)
        }
        return output;
    }
    // Here ends the hacky, hacky MQL implementation
 
 
    // Load a graph based on the results from json.load(file)
    TripleStore.prototype.__generate_json_id = function(){
        this.__json_id += 1;
        return this.__json_id + "";
    }
    
    TripleStore.prototype.load_json = function(json) {
        this.__json_id = 0;
        if (isArray(json))
            for (var x in json)
                this.__json_load_helper(x);
        else if (getType(json) === "object")
            this.__json_load_helper(json);
    }
    TripleStore.prototype.__json_load_helper = function(json) {
        var node;
        if ("id" in json)
            node = this.add_node(json["id"])
        else
            node = this.add_node(this.__generate_json_id())
 
        for (var k in json) {
            var v = json[k];
            var t = this.ns_split_id(k)
            if (t != null) {
                var ns = t[0]; var pred = t[1];
                if (pred === "id") {
                    //only IDs for now
                    if (this.ns[ns] == null)
                        this.add_ns_adapter(ns, NullAdapter)
                    this.__add_ns_id_for_node(ns, v, this.ids[node])
                }
            }
            else {
                if (isArray(v)) {
                    for (var target in v) {
                        if (getType(target) === "object") {
                            t = this.__json_load_helper(target)
                            this.add_link([node, k, t])
                        }
                        else if (isArray(target))
                            throw new Error('JSONLoadException()');
                        else {
                            if (target.startswith(":"))
                                target = this.add_node(target)
                            this.add_link([node, k, target])
                        }
                    }
                }
                else if (getType(v) === "object") {
                    t = this.__json_load_helper(v)
                    this.add_link([node, k, t])
                }
                else if (v == null)
                    continue
                else if (getType(v) === 'number')
                    this.add_link([node, k, v])
                else {
                    if (k === "id")
                        continue
                    if (v.charAt(0) === ":")
                        v = this.add_node(v)
                    this.add_link([node, k, v])
                }
            }
        }
        
        return node;
    }
 
})()
