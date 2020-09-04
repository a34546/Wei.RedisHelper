using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Text;

namespace Wei.RedisHelper
{
    public static class RidisHelperServiceCollectionExtension
    {
        public static IServiceCollection AddRedisHelper(this IServiceCollection services, Action<RedisOption> options)
        {
            if (options == null) throw new ArgumentNullException("options cannot be null");
            var redisOption = new RedisOption();
            options(redisOption);
            services.AddSingleton(redisOption);
            services.AddSingleton(typeof(RedisClient));
            return services;
        }
    }
}
