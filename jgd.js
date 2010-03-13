//warning: completely and totally untested, translated by hand from Python

(function(toplevel) {
    var debug;
    try {
        var sys = require('sys');
        debug = function(msg) {sys.puts(msg);}
    } catch(e) {}
    if (debug === undefined && 'console' in toplevel && toplevel.console.log)
            debug = function(msg) {toplevel.console.log(msg);}
    if (debug === undefined) debug = function(){};
    
    var exports = 'exports' in toplevel ? toplevel.exports : toplevel;
    
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
    
    //for compatability
    function pyForEach(val, onEach) {
        switch(getType(val)) {
            case "array": 
                for (var i = 0; i < val.length; i++)
                    onEach(val[i], i);
                break;
            case "object":
                if (val instanceof Set){
                    pyForEach(val.getAll(), onEach);
                    return;
                }
                for (var key in val)
                    onEach(key, val[key]);
                break;
            default:
                throw new Error("pyForEach doesn't know how to iterate over a " + getType(val));
        }
    }
    
    function pyIn(val, container) {
        switch(getType(container)) {
            case "object": 
                if (container instanceof Set)
                    return container.contains(val);
                return (val in container);
            case "array":
                for (var i = 0; i < container.length; i++)
                    if (val === container[i])
                        return true
                return false;
            default:
                throw new Error("pyIn doesn't know how to detect containment in a " + getType(val) + " pyIn("+JSON.stringify(val)+", "+JSON.stringify(container)+")");
        }
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
      * @param {Array=} arr
      */
    var Set = function(arr) {
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
        this.toJSON = function() {return set}
        if (isArray(arr))
            this.addAll(arr);
    }
    
    function NullAdapter(universal) {}
    NullAdapter.prototype.get_id     = function(search_string, strict_match, var_args) {return null;}
    NullAdapter.prototype.filter     = function(prop, ids, rhs, reverse) {return [new Set(), {}]}
    NullAdapter.prototype.annotate   = function(prop, node_id, data, reverse) {return null;}
    
    
    /** @constructor */
    function TripleStore() {
        //all of the indices
        /** @type Object.<string,number> */
        this.ids = {};
        /** @type Object.<string,Object.<string,number>>*/
        this.ns_ids = {};
        /** @type Object.<string,Array.<Array>>*/
        this.predicates = {};
        /** @type Object.<string,Array.<Array>> */
        this.literal_predicates = {};
        /** @type Object.<string,Array.<Array>> */
        this.literals = {};
        /** @type number */
        this.id_printer = 0;
        /** @type Object.<number, GraphNode>*/
        this.nodes = {};
        /** @type Object.<string, Adapter> */
        this.ns = {};
        /** @type number*/
        this.primitive_count = 0;
        /** @type number*/
        this.__json_id = 0;
    }
    
    /** GraphNode
        {"ids": Array.<number>,
         "links": Object.<string, Array.<number>>},
         "literal_links": Object.<string, Array.<string>>,
         "reverse_links": Object.<string, Array.<number>>},
        }
    */
    
    TripleStore.prototype.add_to_index = function(index, key, value) {
        index[key] = index[key] || [];
        index[key].push(value);
    }
    
    TripleStore.prototype.is_id = function(id) {
        return pyIn(id, this.ids);
    }
    
    TripleStore.prototype.get_id = function(nodenum) {
        return this.nodes[nodenum]['ids'][0]
    }
    
    TripleStore.prototype.is_ns_prop = function(prop) {
        var l = prop.split(":");
        if (l.length >= 2)
            if (pyIn(l[0], this.ns))
                return true;
        return false;
    }
    
//  Add a tuple. Triple is [subject_id, "predicate", object_id]
    TripleStore.prototype.add_link = function(triple) {
        var s = triple[0]; var p = triple[1]; var o = triple[2];
        if (!pyIn(s, this['ids']))
            throw new Error("TripleException"); //FIXME
        s = this.ids[s];
        if (pyIn(o, this.ids)) {
            if (pyIn(p, this.nodes[s].links))
                if (pyIn(o, this.nodes[s].links[p]))
                    return
            this.add_to_index(this.nodes[this.ids[o]].reverse_links, p, s);
            this.add_to_index(this.nodes[s].links, p, this.ids[o]);
            this.add_to_index(this.predicates, p, [s, this.ids[o]]);
        }
        else {
            //o is a literal
            if (pyIn(p, this.nodes[s].literal_links))
                if (pyIn(o, this.nodes[s].literal_links[p]))
                    return
            if (!pyIn(o, this.literals))
                this.literals[o] = {};
            this.add_to_index(this.literals[o], p, s);
            this.add_to_index(this.nodes[s].literal_links, p, o);
            this.add_to_index(this.literal_predicates, p, [s, o]);
        }
        this.primitive_count++;
    }
//  Add a node. Returns the node id.
    TripleStore.prototype.add_node = function(id) {
        id = id + "";
        if (id.charAt(0) !== ":")
            id = ":" + id;
        if (!pyIn(id, this.ids)) {
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
        if (!pyIn(ns, this.ns_ids))
            this.ns_ids[ns] = {};
    }

//  Add data to a query result set 
    TripleStore.prototype.assert_pred_on_query = function(query, pred, target) {
        var self = this;
        var is_list = false;
        var count = 0;
        if (isArray(query)) {
            is_list = true;
            query = query[0];
        }
        var results = this.__filter(query);
        var result_ids = results[0]; var result_data = results[1];
        pyForEach(result_ids, function(i) {
            var tup = [this.get_id(i), pred, target];
            self.add_link(tup);
            count++;            
        });
        return count
    }

    TripleStore.prototype.reify = function(prop, to_prop, query) {}
    TripleStore.prototype.iterate_triples_for_id = function(id) {}
    TripleStore.prototype.remove_link = function(triple) {
        var s = triple[0]; var p = triple[1]; var o = triple[2];
        return;
    }

    TripleStore.prototype.merge = function(from_id, to_id) {
        if (!pyIn(from_id, this.ids) || !pyIn(to_id, this.ids))
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
        var self = this;
        var args = Array.prototype.slice.call(arguments,3);

        if (prop === "id") {
            debug("Temporarily unsupported")
//          XXX Todo: look up the id in the ids index
            return 0;
        }
 
        var predlinks = this.literal_predicates[prop]
 
        if (predlinks === undefined){
            debug("No literal predicate by that name");
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
        pyForEach(predlinks, function(link) {
            var sub = link[0]; var obj = link[1];
            if (filterlist !== null && !pyIn(sub, filterlist))
                return //read as continue
            pyForEach(nslist, function(namespace) {
                var ns_obj = this.ns[namespace]
                var id_out = ns_obj.get_id.apply(ns_obj, [obj].concat(args))
                if (id_out !== null) {
                    self.__add_ns_id_for_node(namespace, id_out, sub)
                    n_ids += 1
                }
            })
        })
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
        var self = this;
        if (nums === undefined) {
            if (this.ns[ns].universal)
                nums = getKeys(this.nodes);
            else
                nums = getKeys(this.ns_ids[ns]);
        }
        pyForEach(nums, function(n) {
            if (!pyIn(n, self.ns_ids[ns])) {
                if (self.ns[ns].universal) {
                    pyForEach(self.nodes[n]["ids"], function(key) {
                        var t = self.ns_split_id(key);
                        if (t === null)
                            out.push([n, key.substring(1)]);
                    })
                }
                return;
            }
            pyForEach(self.ns_ids[ns][n], function(key) {
                var split_id = self.ns_split_id(key);
                ns = split_id[0]; var v = split_id[1];
                out.push([n, v])
            })
        })
        return out
    }

    TripleStore.prototype.__add_ns_id_for_node = function(ns, id, nodenum) {
        id = id + "";
        ns = ns + "";
        if (!pyIn(ns, this.ns))
            throw new Error("IDException");
        if (id.charAt(0) !== ":")
            id = ":" + id;
        var ns_id = ":" + ns + id;
        if (pyIn(ns_id, this.ids))
            return ns_id;
        if (pyIn(nodenum, this.nodes)) {
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
        for (var x in idlist) {
            var id = idlist[x];
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
            pyForEach(rl, function(p) {
                pyForEach(rl[p], function(l) {
                    count += 1;
                })
            })
            if (count === 1 && !pyIn(o, done_nodes))
                return serialize_node(o);
            return self.nodes[o]["ids"][0];
        }
        function serialize_node(nn) {
            var this_data = {};
            pyForEach(self.nodes[nn]["literal_links"], function(key) {
                var index = self.nodes[nn]["literal_links"][key];
                var d = toArray(index);
                if (d.length == 1)
                    d = d[0];
                this_data[key] = d;
            })
            
            pyForEach(self.nodes[nn]["ids"], function(id) {
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
            })
            
            done_nodes[nn] = true
            pyForEach(self.nodes[nn]["links"], function(key) {
                var index = self.nodes[nn]["links"][key];
                var d = [];
                pyForEach(index, function(x) {
                    d.push(pre_serialize_node(nn, key, x));
                })
                if (d.length === 1)
                    d = d[0];
                this_data[key] = d;
            })
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
        var self = this;
        for (var pred in q) {
            var target = q[pred];
            var reverse = false
            var this_intermediate = new Set()
            if (pred.charAt(0) ==="!") {
                reverse = true;
                pred = pred.substring(1);
            }
            if (self.ns_split_id(pred) !== null) {
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
                    if (self.ids[target] !== undefined)
                        this_intermediate = new Set([self.ids[target]]);
                    else
                        return [new Set(), {}];
                }
                else {
                    if (self.is_id(target)) {
                        index = self.nodes[self.ids[target]]["reverse_links"][pred]
                        if (reverse)
                            index = self.nodes[self.ids[target]]["links"][pred];
                    }
                    else {
                        if (reverse)
                            throw new Error('QueryError("Literal cannot be reversed")');
                        index = self.literals[target];
                        if (index !== undefined)
                            index = index[pred];
                    }
                    if (index === undefined)
                        return [new Set(), {}];
                    pyForEach(index, function(id) {
                        this_intermediate.add(id);
                    })
                }
            }
            if (isArray(target))
                target = target[0];
            if (getType(target) === "object") {
                if (!pyIn(pred, self.predicates))
                    return [new Set(), {}]
                var sub_obj = self.predicates[pred];
                var filtered = self.__filter(target);
                var linkage_ids = filtered[0]; var linkage_data = filtered[1];
                if (reverse)
                    pred = "!" + pred;
                pyForEach(sub_obj, function(s_o) {
                    var s = s_o[0]; var o = s_o[1];
                    if (reverse) {
                        var o_temp = s;
                        s = o;
                        o = o_temp;
                    }
                    if (pyIn(o, linkage_ids)) {
                        this_intermediate.add(s)
                        if (id_data[s] === undefined)
                            id_data[s] = {}
                        if (id_data[s][pred] === undefined)
                            id_data[s][pred] = {}
                        id_data[s][pred][o] = linkage_data[o]
                    }
                })
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
            for (var pred in q) {
                var target = q[pred];
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
                        pyForEach(fid_list, function(x) {
                            selected_ids.add(this.ids[":" + ns + ":" + x]);
                        })
                        // how could x ever be None here? [x for x in selected_ids if x is not None]
                        if (final_ids == null)
                            final_ids = selected_ids
                        else
                            final_ids = final_ids.intersection(selected_ids)
                    }
                    continue;
                }

                var adapter = self.ns[ns]
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
                pyForEach(final_ids, function(x) {
                    if (id_data[x] === undefined)
                        id_data[x] = {}
                    id_data[x][pred] = adapter_data[x];
                })
            }
        }
        
        var data_out = {};
        if (final_ids == null)
            final_ids = new Set(getKeys(this.nodes));
        pyForEach(final_ids, function(x) {
            data_out[x] = id_data[x];
        })
        return [final_ids, data_out];
    }

    // Given a query and output from filter, fill in results and cheat where possible. 
    // If it's filtered correctly, then the literal 
    // values must be the same. The None values and dict/lists can be reinterpreted
    // recursively by looking up, for the given results' node number at this level
    TripleStore.prototype.__annotate = function(q, out_set, out_data) {
        var output = []
        var foreign_keys = false
        var self = this;
        pyForEach(out_set, function(x) {
            var data = {}
            var node = self.nodes[x]
            var filter_data = out_data[x]
            pyForEach(q, function(pred) {
                var target = q[pred];
                if (self.ns_split_id(pred) != null) {
                    foreign_keys = true;
                    return;
                }
                if (getType(target) === "string" || getType(target) === "number")
                    data[pred] = target
                else if (isArray(target)) {
                    if (filter_data[pred] == null)
                        data[pred] = []
                    else
                        data[pred] = self.__annotate(target[0], getKeys(filter_data[pred]), filter_data[pred])
                }
                else if (getType(target) == "object"){
                    if (filter_data[pred] == null)
                        data[pred] = null
                    else
                        data[pred] = self.__annotate(target, getKeys(filter_data[pred]), filter_data[pred])[0]
                }
                else if (target == null) {
                    if (pred == "id") {
                        data[pred] = node["ids"][0];
                        return;
                    }
                    var forelinks = node["links"][pred];
                    if (forelinks != null){
                        //ids
                        if (forelinks.length == 1)
                            data[pred] = self.nodes[forelinks[0]]["ids"][0];
                        else {
                            var temp_array = [];
                            pyForEach(forelinks, function(x) {
                                temp_array.push(self.nodes[x]['ids'][0])
                            })
                            data[pred] = temp_array;
                        }
                        return;
                    }
                    forelinks = node["literal_links"][pred];
                    if (forelinks != null) {
                        //strs
                        if (forelinks.length == 1)
                            data[pred] = forelinks[0]
                        else
                            data[pred] = Array.prototype.slice.call(forelinks);
                        return;
                    }
                    data[pred] = null;
                }
                else
                    throw new Error('QueryException()');
            })
            
        // This is the block needed to call out into an adapter. Could be taken out
        // to simplify the MQL implementation
            if (foreign_keys) {
                for (var pred in q) {
                    var target = q[pred];
                    var reverse = false;
                    var barepred = pred;
                    if (barepred.charAt(0) === "!") {
                        reverse = true;
                        barepred = barepred.substring(1);
                    }
                    var t = this.ns_split_id(barepred);
                    if (getType(target) === "string" || getType(target) === 'number') {
                        data[pred] = target;
                        return;
                    }
                    if (t === null)
                        return;
                    var ns = t[0]; var predicate = t[1];
                    if (predicate == "id") {
                         t = self.ns_ids[ns][x]
                        if (t == null) {
                            data[pred] = null
                            if (target == [])
                                data[pred] = [];
                        }
                        if (t != null) {
                            var temp_array = [];
                            pyForEach(t, function(x) {
                                temp_array.push(self.ns_split_id(x)[1]);
                            })
                            data[pred] = temp_array;
                            if (target == null)
                                data[pred] = data[pred][0]
                        }
                        return;
                    }
                    data[pred] = self.ns[ns].annotate(predicate, x, filter_data[pred], reverse=reverse)
                }
            }
            output.push(data)
        })
        return output;
    }
    // Here ends the hacky, hacky MQL implementation
 
 
    // Load a graph based on the results from json.load(file)
    TripleStore.prototype.__generate_json_id = function(){
        this.__json_id += 1;
        return this.__json_id + "";
    }
    
    TripleStore.prototype.load_json = function(json) {
        var self = this;
        if (isArray(json))
            pyForEach(json, function(x) {
                self.__json_load_helper(x);
            })
        else if (getType(json) === "object")
            this.__json_load_helper(json);
    }
    TripleStore.prototype.__json_load_helper = function(json) {
        var node;
        var self = this;
        if (pyIn("id", json))
            node = this.add_node(json["id"])
        else
            node = this.add_node(this.__generate_json_id())
 
        pyForEach(json, function(k) {
            var v = json[k];
            var t = self.ns_split_id(k)
            if (t != null) {
                var ns = t[0]; var pred = t[1];
                if (pred === "id") {
                    //only IDs for now
                    if (self.ns[ns] == null)
                        self.add_ns_adapter(ns, NullAdapter)
                    self.__add_ns_id_for_node(ns, v, self.ids[node])
                }
            }
            else {
                if (isArray(v)) {
                    pyForEach(v, function(target) {
                        if (getType(target) === "object") {
                            t = self.__json_load_helper(target)
                            self.add_link([node, k, t])
                        }
                        else if (isArray(target))
                            throw new Error('JSONLoadException()');
                        else {
                            if (target.charAt(0) === ":")
                                target = self.add_node(target)
                            self.add_link([node, k, target])
                        }
                    })
                }
                else if (getType(v) === "object") {
                    t = self.__json_load_helper(v)
                    self.add_link([node, k, t])
                }
                else if (v == null)
                    return; //i.e. continue
                else if (getType(v) === 'number')
                    self.add_link([node, k, v])
                else {
                    if (k === "id")
                        return; //i.e. continue
                    if (v.charAt(0) === ":")
                        v = self.add_node(v)
                    self.add_link([node, k, v])
                }
            }
        })
        
        return node;
    }
    exports.TripleStore = TripleStore;
})(this)
