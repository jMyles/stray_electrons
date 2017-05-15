import redis

rpi_redis = redis.Redis(host="10.0.80.30")

rpi_redis.set('lembas', 'dingos')
pass