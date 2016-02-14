function strToScore(str, ignoreCase) {
    if ((typeof ignoreCase) === 'undefined') {
        ignoreCase = false;
    }

    if (ignoreCase) {
        str = str.toLowerCase();
    }

    var strArr = str.split('');

    var asciiObj = strArr.map(function(currentValue) {
        return currentValue.charCodeAt(0);
    });

    while (asciiObj.length < 6) {
        asciiObj.push(-1);
    }

    var score = 0;
    asciiObj.forEach(function(item) {
        score = score * 257 + item + 1;
    });

    return score * 2 + (str.length > 6);
}
