using Newtonsoft.Json;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace Wei.RedisHelper
{
    public class RedisClient
    {
        private readonly ConnectionMultiplexer _connectionMultiplexer;
        private readonly RedisOption _option;
        private IDatabase _database;
        private static readonly object locker = new object();

        public RedisClient(RedisOption option)
        {
            _option = option;
            _connectionMultiplexer = ConnectionMultiplexer.Connect(option.RedisConnectionString);
        }

        public IDatabase Database
        {
            get
            {
                if (_database == null)
                {
                    lock (locker)
                    {
                        if (_database == null)
                        {
                            _database = _connectionMultiplexer.GetDatabase(_option.DbNum, _option.AsyncState);
                        }
                    }
                }
                return _database;
            }
        }

        public IDatabase GetDatabase(int dbNum = -1, object asyncState = null)
        {
            return _connectionMultiplexer.GetDatabase(dbNum, asyncState);
        }

        public ITransaction CreateTransaction()
        {
            return Database.CreateTransaction();
        }

        #region Private

        private string ConvertJson<T>(T value)
        {
            string result = value is string ? value.ToString() : JsonConvert.SerializeObject(value);
            return result;
        }

        private RedisKey[] ConvertRedisKeys(List<string> redisKeys)
        {
            return redisKeys.Select(redisKey => (RedisKey)redisKey).ToArray();
        }

        private T ConvertObj<T>(RedisValue value)
        {
            if (value.IsNullOrEmpty)
                return default;
            return JsonConvert.DeserializeObject<T>(value);
        }

        private List<T> ConvetList<T>(RedisValue[] values)
        {
            List<T> result = new List<T>();
            foreach (var item in values)
            {
                var model = ConvertObj<T>(item);
                result.Add(model);
            }
            return result;
        }

        #endregion

        #region String

        /// <summary>
        /// 保存单个key value
        /// </summary>
        /// <param name="key">Redis Key</param>
        /// <param name="value">保存的值</param>
        /// <param name="expiry">过期时间</param>
        /// <returns></returns>
        public bool StringSet(string key, string value, TimeSpan? expiry = default)
        {
            return Database.StringSet(key, value, expiry);
        }

        /// <summary>
        /// 保存单个key value
        /// </summary>
        /// <param name="key">Redis Key</param>
        /// <param name="value">保存的值</param>
        /// <param name="expiry">过期时间</param>
        /// <returns></returns>
        public Task<bool> StringSetAsync(string key, string value, TimeSpan? expiry = default)
        {
            return Database.StringSetAsync(key, value, expiry);
        }

        /// <summary>
        /// 保存多个key value
        /// </summary>
        /// <param name="keyValues">键值对</param>
        /// <returns></returns>
        public bool StringSet(List<KeyValuePair<RedisKey, RedisValue>> keyValues)
        {
            return Database.StringSet(keyValues.ToArray());
        }

        /// <summary>
        /// 保存多个key value
        /// </summary>
        /// <param name="keyValues">键值对</param>
        /// <returns></returns>
        public Task<bool> StringSetAsync(List<KeyValuePair<RedisKey, RedisValue>> keyValues)
        {
            return Database.StringSetAsync(keyValues.ToArray());
        }


        /// <summary>
        /// 保存一个对象
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <param name="obj"></param>
        /// <param name="expiry"></param>
        /// <returns></returns>
        public bool StringSet<T>(string key, T obj, TimeSpan? expiry = default)
        {
            string json = ConvertJson(obj);
            return Database.StringSet(key, json, expiry);
        }

        /// <summary>
        /// 保存一个对象
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <param name="obj"></param>
        /// <param name="expiry"></param>
        /// <returns></returns>
        public Task<bool> StringSetAsync<T>(string key, T obj, TimeSpan? expiry = default)
        {
            string json = ConvertJson(obj);
            return Database.StringSetAsync(key, json, expiry);
        }

        /// <summary>
        /// 获取单个key的值
        /// </summary>
        /// <param name="key">Redis Key</param>
        /// <returns></returns>
        public string StringGet(string key)
        {
            return Database.StringGet(key);
        }

        /// <summary>
        /// 获取单个key的值
        /// </summary>
        /// <param name="key">Redis Key</param>
        /// <returns></returns>
        public async Task<string> StringGetAsync(string key)
        {
            return await Database.StringGetAsync(key);
        }

        /// <summary>
        /// 获取多个Key
        /// </summary>
        /// <param name="listKey">Redis Key集合</param>
        /// <returns></returns>
        public RedisValue[] StringGet(List<string> listKey)
        {
            return Database.StringGet(ConvertRedisKeys(listKey));
        }

        /// <summary>
        /// 获取多个Key
        /// </summary>
        /// <param name="listKey">Redis Key集合</param>
        /// <returns></returns>
        public Task<RedisValue[]> StringGetAsync(List<string> listKey)
        {
            return Database.StringGetAsync(ConvertRedisKeys(listKey));
        }

        /// <summary>
        /// 获取一个key的对象
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <returns></returns>
        public T StringGet<T>(string key)
        {
            return ConvertObj<T>(Database.StringGet(key));
        }

        /// <summary>
        /// 获取一个key的对象
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <returns></returns>
        public async Task<T> StringGetAsync<T>(string key)
        {
            return ConvertObj<T>(await Database.StringGetAsync(key));
        }

        /// <summary>
        /// 为数字增长val
        /// </summary>
        /// <param name="key"></param>
        /// <param name="val">可以为负</param>
        /// <returns>增长后的值</returns>
        public long StringIncrement(string key, long val = 1)
        {
            return Database.StringIncrement(key, val);
        }

        /// <summary>
        /// 为数字增长val
        /// </summary>
        /// <param name="key"></param>
        /// <param name="val">可以为负</param>
        /// <returns>增长后的值</returns>
        public Task<long> StringIncrementAsync(string key, long val = 1)
        {
            return Database.StringIncrementAsync(key, val);
        }

        /// <summary>
        /// 为数字减少val
        /// </summary>
        /// <param name="key"></param>
        /// <param name="val">可以为负</param>
        /// <returns>减少后的值</returns>
        public long StringDecrement(string key, long val = 1)
        {
            return Database.StringDecrement(key, val);
        }

        /// <summary>
        /// 为数字减少val
        /// </summary>
        /// <param name="key"></param>
        /// <param name="val">可以为负</param>
        /// <returns>减少后的值</returns>
        public Task<long> StringDecrementAsync(string key, long val = 1)
        {
            return Database.StringDecrementAsync(key, val);
        }

        #endregion

        #region Hash

        /// <summary>
        /// 判断某个数据是否已经被缓存
        /// </summary>
        /// <param name="key"></param>
        /// <param name="dataKey"></param>
        /// <returns></returns>
        public bool HashExists(string key, string dataKey)
        {
            return Database.HashExists(key, dataKey);
        }

        /// <summary>
        /// 判断某个数据是否已经被缓存
        /// </summary>
        /// <param name="key"></param>
        /// <param name="dataKey"></param>
        /// <returns></returns>
        public Task<bool> HashExistsAsync(string key, string dataKey)
        {
            return Database.HashExistsAsync(key, dataKey);
        }

        /// <summary>
        /// 存储数据到hash表
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <param name="dataKey"></param>
        /// <param name="t"></param>
        /// <returns></returns>
        public bool HashSet<T>(string key, string dataKey, T t)
        {
            return Database.HashSet(key, dataKey, ConvertJson(t));
        }

        /// <summary>
        /// 存储数据到hash表
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <param name="dataKey"></param>
        /// <param name="t"></param>
        /// <returns></returns>
        public Task<bool> HashSetAsync<T>(string key, string dataKey, T t)
        {
            return Database.HashSetAsync(key, dataKey, ConvertJson(t));
        }

        /// <summary>
        /// 移除hash中的某值
        /// </summary>
        /// <param name="key"></param>
        /// <param name="dataKey"></param>
        /// <returns></returns>
        public bool HashDelete(string key, string dataKey)
        {
            return Database.HashDelete(key, dataKey);
        }

        /// <summary>
        /// 移除hash中的某值
        /// </summary>
        /// <param name="key"></param>
        /// <param name="dataKey"></param>
        /// <returns></returns>
        public Task<bool> HashDeleteAsync(string key, string dataKey)
        {
            return Database.HashDeleteAsync(key, dataKey);
        }

        /// <summary>
        /// 移除hash中的多个值
        /// </summary>
        /// <param name="key"></param>
        /// <param name="dataKeys"></param>
        /// <returns></returns>
        public long HashDelete(string key, List<RedisValue> dataKeys)
        {
            return Database.HashDelete(key, dataKeys.ToArray());
        }

        /// <summary>
        /// 移除hash中的多个值
        /// </summary>
        /// <param name="key"></param>
        /// <param name="dataKeys"></param>
        /// <returns></returns>
        public Task<long> HashDeleteAsync(string key, List<RedisValue> dataKeys)
        {
            return Database.HashDeleteAsync(key, dataKeys.ToArray());
        }

        /// <summary>
        /// 从hash表获取数据
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <param name="dataKey"></param>
        /// <returns></returns>
        public T HashGet<T>(string key, string dataKey)
        {
            return ConvertObj<T>(Database.HashGet(key, dataKey));
        }

        /// <summary>
        /// 从hash表获取数据
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <param name="dataKey"></param>
        /// <returns></returns>
        public async Task<T> HashGetAsync<T>(string key, string dataKey)
        {
            return ConvertObj<T>(await Database.HashGetAsync(key, dataKey));
        }

        /// <summary>
        /// 为数字增长val
        /// </summary>
        /// <param name="key"></param>
        /// <param name="dataKey"></param>
        /// <param name="val">可以为负</param>
        /// <returns>增长后的值</returns>
        public double HashIncrement(string key, string dataKey, double val = 1)
        {
            return Database.HashIncrement(key, dataKey, val);
        }

        /// <summary>
        /// 为数字增长val
        /// </summary>
        /// <param name="key"></param>
        /// <param name="dataKey"></param>
        /// <param name="val">可以为负</param>
        /// <returns>增长后的值</returns>
        public Task<double> HashIncrementAsync(string key, string dataKey, double val = 1)
        {
            return Database.HashIncrementAsync(key, dataKey, val);
        }

        /// <summary>
        /// 为数字减少val
        /// </summary>
        /// <param name="key"></param>
        /// <param name="dataKey"></param>
        /// <param name="val">可以为负</param>
        /// <returns>减少后的值</returns>
        public double HashDecrement(string key, string dataKey, double val = 1)
        {
            return Database.HashDecrement(key, dataKey, val);
        }

        /// <summary>
        /// 为数字减少val
        /// </summary>
        /// <param name="key"></param>
        /// <param name="dataKey"></param>
        /// <param name="val">可以为负</param>
        /// <returns>减少后的值</returns>
        public Task<double> HashDecrementAsync(string key, string dataKey, double val = 1)
        {
            return Database.HashDecrementAsync(key, dataKey, val);
        }

        /// <summary>
        /// 获取hashkey所有Redis key
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <returns></returns>
        public List<T> HashKeys<T>(string key)
        {
            return ConvetList<T>(Database.HashKeys(key));
        }

        /// <summary>
        /// 获取hashkey所有Redis key
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <returns></returns>
        public async Task<List<T>> HashKeysAsync<T>(string key)
        {
            return ConvetList<T>(await Database.HashKeysAsync(key));
        }

        public List<T> HashValues<T>(string key)
        {
            return ConvetList<T>(Database.HashValues(key));
        }

        public async Task<List<T>> HashValuesAsync<T>(string key)
        {
            return ConvetList<T>(await Database.HashValuesAsync(key));
        }

        #endregion

        #region List

        /// <summary>
        /// 移除指定ListId的内部List的值
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public long ListRemove<T>(string key, T value)
        {
            return Database.ListRemove(key, ConvertJson(value));
        }

        /// <summary>
        /// 移除指定ListId的内部List的值
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public Task<long> ListRemoveAsync<T>(string key, T value)
        {
            return Database.ListRemoveAsync(key, ConvertJson(value));
        }

        /// <summary>
        /// 获取指定key的List
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public List<T> ListRange<T>(string key)
        {
            return ConvetList<T>(Database.ListRange(key));
        }

        /// <summary>
        /// 获取指定key的List
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public async Task<List<T>> ListRangeAsync<T>(string key)
        {
            return ConvetList<T>(await Database.ListRangeAsync(key));
        }

        /// <summary>
        /// 入队
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public long ListRightPush<T>(string key, T value)
        {
            return Database.ListRightPush(key, ConvertJson(value));
        }

        /// <summary>
        /// 入队
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public Task<long> ListRightPushAsync<T>(string key, T value)
        {
            return Database.ListRightPushAsync(key, ConvertJson(value));
        }

        /// <summary>
        /// 出队
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <returns></returns>
        public T ListRightPop<T>(string key)
        {
            return ConvertObj<T>(Database.ListRightPop(key));
        }

        /// <summary>
        /// 出队
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <returns></returns>
        public async Task<T> ListRightPopAsync<T>(string key)
        {
            return ConvertObj<T>(await Database.ListRightPopAsync(key));
        }

        /// <summary>
        /// 入栈
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public long ListLeftPush<T>(string key, T value)
        {
            return Database.ListLeftPush(key, ConvertJson(value));
        }

        /// <summary>
        /// 入栈
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public Task<long> ListLeftPushAsync<T>(string key, T value)
        {
            return Database.ListLeftPushAsync(key, ConvertJson(value));
        }

        /// <summary>
        /// 出栈
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <returns></returns>
        public T ListLeftPop<T>(string key)
        {
            return ConvertObj<T>(Database.ListLeftPop(key));
        }

        /// <summary>
        /// 出栈
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <returns></returns>
        public async Task<T> ListLeftPopAsync<T>(string key)
        {
            return ConvertObj<T>(await Database.ListLeftPopAsync(key));
        }

        /// <summary>
        /// 获取集合中的数量
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public long ListLength(string key)
        {
            return Database.ListLength(key);
        }

        /// <summary>
        /// 获取集合中的数量
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public Task<long> ListLengthAsync(string key)
        {
            return Database.ListLengthAsync(key);
        }

        #endregion

        #region SortedSet

        /// <summary>
        /// 添加
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="score"></param>
        public bool SortedSetAdd<T>(string key, T value, double score)
        {
            return Database.SortedSetAdd(key, ConvertJson<T>(value), score);
        }

        /// <summary>
        /// 添加
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="score"></param>
        public Task<bool> SortedSetAddAsync<T>(string key, T value, double score)
        {
            return Database.SortedSetAddAsync(key, ConvertJson<T>(value), score);
        }

        /// <summary>
        /// 删除
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        public bool SortedSetRemove<T>(string key, T value)
        {
            return Database.SortedSetRemove(key, ConvertJson(value));
        }

        /// <summary>
        /// 删除
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        public Task<bool> SortedSetRemoveAsync<T>(string key, T value)
        {
            return Database.SortedSetRemoveAsync(key, ConvertJson(value));
        }

        /// <summary>
        /// 获取全部
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public List<T> SortedSetRangeByRank<T>(string key)
        {
            return ConvetList<T>(Database.SortedSetRangeByRank(key));
        }

        /// <summary>
        /// 获取全部
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public async Task<List<T>> SortedSetRangeByRankAsync<T>(string key)
        {
            return ConvetList<T>(await Database.SortedSetRangeByRankAsync(key));
        }

        /// <summary>
        /// 获取集合中的数量
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public long SortedSetLength(string key)
        {
            return Database.SortedSetLength(key);
        }

        /// <summary>
        /// 获取集合中的数量
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public Task<long> SortedSetLengthAsync(string key)
        {
            return Database.SortedSetLengthAsync(key);
        }

        #endregion

        #region key

        /// <summary>
        /// 删除单个key
        /// </summary>
        /// <param name="key">redis key</param>
        /// <returns>是否删除成功</returns>
        public bool KeyDelete(string key)
        {
            return Database.KeyDelete(key);
        }

        /// <summary>
        /// 删除单个key
        /// </summary>
        /// <param name="key">redis key</param>
        /// <returns>是否删除成功</returns>
        public Task<bool> KeyDeleteAsync(string key)
        {
            return Database.KeyDeleteAsync(key);
        }

        /// <summary>
        /// 删除多个key
        /// </summary>
        /// <param name="keys">rediskey</param>
        /// <returns>成功删除的个数</returns>
        public long KeyDelete(List<string> keys)
        {
            return Database.KeyDelete(ConvertRedisKeys(keys));
        }

        /// <summary>
        /// 删除多个key
        /// </summary>
        /// <param name="keys">rediskey</param>
        /// <returns>成功删除的个数</returns>
        public Task<long> KeyDeleteAsync(List<string> keys)
        {
            return Database.KeyDeleteAsync(ConvertRedisKeys(keys));
        }

        /// <summary>
        /// 判断key是否存储
        /// </summary>
        /// <param name="key">redis key</param>
        /// <returns></returns>
        public bool KeyExists(string key)
        {
            return Database.KeyExists(key);
        }

        /// <summary>
        /// 判断key是否存储
        /// </summary>
        /// <param name="key">redis key</param>
        /// <returns></returns>
        public Task<bool> KeyExistsAsync(string key)
        {
            return Database.KeyExistsAsync(key);
        }

        /// <summary>
        /// 重新命名key
        /// </summary>
        /// <param name="key">就的redis key</param>
        /// <param name="newKey">新的redis key</param>
        /// <returns></returns>
        public bool KeyRename(string key, string newKey)
        {
            return Database.KeyRename(key, newKey);
        }

        /// <summary>
        /// 重新命名key
        /// </summary>
        /// <param name="key">就的redis key</param>
        /// <param name="newKey">新的redis key</param>
        /// <returns></returns>
        public Task<bool> KeyRenameAsync(string key, string newKey)
        {
            return Database.KeyRenameAsync(key, newKey);
        }

        /// <summary>
        /// 设置Key的时间
        /// </summary>
        /// <param name="key">redis key</param>
        /// <param name="expiry"></param>
        /// <returns></returns>
        public bool KeyExpire(string key, TimeSpan? expiry = default)
        {
            return Database.KeyExpire(key, expiry);
        }

        /// <summary>
        /// 设置Key的时间
        /// </summary>
        /// <param name="key">redis key</param>
        /// <param name="expiry"></param>
        /// <returns></returns>
        public Task<bool> KeyExpireAsync(string key, TimeSpan? expiry = default)
        {
            return Database.KeyExpireAsync(key, expiry);
        }

        #endregion

        #region Publish/Subscribe

        /// <summary>
        /// 订阅
        /// </summary>
        /// <param name="subChannel"></param>
        /// <param name="handler"></param>
        public void Subscribe(string subChannel, Action<RedisChannel, RedisValue> handler = null)
        {
            ISubscriber sub = _connectionMultiplexer.GetSubscriber();
            sub.Subscribe(subChannel, (channel, message) =>
            {
                handler?.Invoke(channel, message);
            });
        }

        /// <summary>
        /// 订阅
        /// </summary>
        /// <param name="subChannel"></param>
        /// <param name="handler"></param>
        public Task SubscribeAsync(string subChannel, Action<RedisChannel, RedisValue> handler = null)
        {
            var sub = _connectionMultiplexer.GetSubscriber();
            return sub.SubscribeAsync(subChannel, (channel, message) =>
            {
                handler?.Invoke(channel, message);
            });
        }

        /// <summary>
        /// 发布
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="channel"></param>
        /// <param name="msg"></param>
        /// <returns></returns>
        public long Publish<T>(string channel, T msg)
        {
            var sub = _connectionMultiplexer.GetSubscriber();
            return sub.Publish(channel, ConvertJson(msg));
        }

        /// <summary>
        /// 发布
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="channel"></param>
        /// <param name="msg"></param>
        /// <returns></returns>
        public Task<long> PublishAsync<T>(string channel, T msg)
        {
            var sub = _connectionMultiplexer.GetSubscriber();
            return sub.PublishAsync(channel, ConvertJson(msg));
        }

        /// <summary>
        /// 取消订阅
        /// </summary>
        /// <param name="channel"></param>
        public void Unsubscribe(string channel)
        {
            var sub = _connectionMultiplexer.GetSubscriber();
            sub.Unsubscribe(channel);
        }

        /// <summary>
        /// 取消订阅
        /// </summary>
        /// <param name="channel"></param>
        public Task UnsubscribeAsync(string channel)
        {
            var sub = _connectionMultiplexer.GetSubscriber();
            return sub.UnsubscribeAsync(channel);
        }

        /// <summary>
        /// 取消全部订阅
        /// </summary>
        public void UnsubscribeAll()
        {
            var sub = _connectionMultiplexer.GetSubscriber();
            sub.UnsubscribeAll();
        }

        /// <summary>
        /// 取消全部订阅
        /// </summary>
        public Task UnsubscribeAllAsync()
        {
            ISubscriber sub = _connectionMultiplexer.GetSubscriber();
            return sub.UnsubscribeAllAsync();
        }

        #endregion
    }
}
