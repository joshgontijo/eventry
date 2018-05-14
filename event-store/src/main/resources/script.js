
var _store;

var main = function(store) {

    var Script = Java.type('io.joshworks.fstore.es.script.Script');

    _store = store;
    state.person.name = "yolo";

    stream.forEach(function(e) {
        print('Hi there from Javascript, ' + e);
    });

    return state;
};

var userApi = function (store) {

};

var fromStream = function(stream, handler) {

    Script.fromStream(_store, stream, handler)


};

fromStream("stream1").forEach(function (event, state) {

});