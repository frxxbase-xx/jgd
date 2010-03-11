//warning: completely and totally untested, translated by hand from Python

(function(undefined) {
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
        return this.nodes[nodenum].ids[0]
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
        if (!(s in this.ids))
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
        var q;
        if isArray(q) {
            is_list = true
            q = q[0]
        }
        var results = this.__filter(q);
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
        if (!(from_id in this.ids) || !(to_id in this.ids)
            throw Error("IDException");
        var from_nn = this.ids[from_id];
        var to_nn = this.ids[to_id];
        if (from_nn === to_nn):
            return
        //Move each link, carefully. 
        //Move each ID
    }
})()
