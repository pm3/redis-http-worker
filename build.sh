docker build -t zooza.azurecr.io/redis-http-worker:latest --push .
docker buildx build --platform linux/arm64 -t zooza.azurecr.io/redis-http-worker:arm64 --push .