var TripleStore = require("./jgd").TripleStore;
var sys = require("sys");
var fs = require("fs");

var ts = new TripleStore();
var test1 = JSON.parse(fs.readFileSync("test1.jgd"));
ts.load_json(test1);

sys.puts(JSON.stringify(ts.MQL([{"id":null}])))
sys.puts("Ok");
