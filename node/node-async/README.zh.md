Redis in Action (NodeJS)

## 注意事项
原Python代码中，有很多切一个线程去做某些事的，这部分代码我做了调整，在NodeJS中需要使用其他的方法来解决这个问题。

### chapter2
- updateToken: 个人觉得使用cookie作为标识符不太合理，每次登录后cookie都会不同，之前使用的cookie对应的数据不会及时清除。可以考虑使用用户对应的全局唯一性ID 或者 updateToken时迁移之前的数据

- JC note: the creation of a mapping of used token cookies per-user via `zadd user_cookie:<username> <timestamp> <uuid>` for auditing / clearing / analytics was left as an exercise to the reader ;)

### chapter5
- logCommon：在源python代码中，为了减少客户端与Redis服务器之间的通信往返次数，使用了在事务中使用了pipelint。但我目前使用的ioredis，如果要在事务中使用pipeline，必须是链式调用的方式，在这种情况下，logRecent中的命令就不是使用pipeline的。
