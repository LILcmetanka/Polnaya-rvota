var x = 5;
function f(n) {
    while (n > 0) {
        console.log(n);
        n--;
    }
    return x;
}
console.log(f(3));
