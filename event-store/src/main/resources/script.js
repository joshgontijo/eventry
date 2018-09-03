options({
    key: "value",
    anotherKey: "AanotherValue"
});
state.oddSum = 0;
state.evenSum = 0;
fromStreams(["odd", "even"])
    .forEach(function(event) {
        state.sum += event.data.age;
        if(event.data.age % 2 === 0) {
            linkTo("even", event)
        } else {
            linkTo("odd", event)
        }
    });