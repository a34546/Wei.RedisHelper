# Wei.RedisHelper
.NetCore RedisHelper ,封装Redis常用基本操作

## 快速开始

> Nuget引用包：Wei.RedisHelper

### 注入服务

```cs
services.AddRedisHelper(ops=> {
	ops.RedisConnectionString = "127.0.0.1:6379,defaultDatabase=1,password=123456";
});
```

### 依赖注入RedisClient使用

```cs
readonly RedisClient _client;
public ValuesController(RedisClient client)
{
	_client = client;
	_logger = logger;
}

[HttpGet("StringSet")]
public Task StringSet()
{
	return _client.StringSetAsync("key", "value");
}
```
### 抢红包Demo
>  https://github.com/a34546/RedisDemo

>  其他方法就不展示了，自己看源码
