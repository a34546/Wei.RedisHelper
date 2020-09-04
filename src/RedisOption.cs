using System;
using System.Collections.Generic;
using System.Text;

namespace Wei.RedisHelper
{
    public class RedisOption
    {
        /// <summary>
        /// Ridis连接字符串
        /// </summary>
        public string RedisConnectionString { get; set; }

        /// <summary>
        /// Ridis数据库序号，默认为-1
        /// </summary>
        public int DbNum { get; set; } = -1;

        /// <summary>
        /// The async state to pass into the resulting StackExchange.Redis.RedisDatabase.
        /// </summary>
        public object AsyncState { get; set; } = null;
    }
}
