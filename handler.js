'use strict';

function executor(params) {
    return new Promise((resolve, reject) => {
        const name = params.name || 'World';
        console.log("NAME: " + name);
        console.log(params);
        resolve(
            {message: 'Hello ' + name}
        );
    });
}

exports.executor = executor;
