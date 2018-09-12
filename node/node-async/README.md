Redis in Action (NodeJS Example code for the book)

## Install
```
yarn install
```

## Run Test
```
npm test
```

if you want to test one file, for example:
```
npm test .\test\ch04.test.js
```

## About Code
### src
- ch**/main.js is source code in NodeJS
- ch**/index.js just used for debug

### test
- *.test.js is test code

### Other programming languages Code
[Example Code](https://github.com/josiahcarlson/redis-in-action)

## Test Background
- Windows 10 and node v8.11.3

## Note
### sleep func
We use sleep in test code, In my opinion, don't use sleep func in our service code, it's will block our service response.
We use sleep just for test.

### Transaction and Pipeline
Now I'm using ioredis, you can see the [transaction](https://github.com/luin/ioredis#transaction) and [pipeline](https://github.com/luin/ioredis#pipelining).