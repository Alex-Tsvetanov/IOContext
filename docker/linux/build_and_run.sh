docker context use default
docker buildx build --platform linux/amd64 -t appframe-dev:latest -f ./Dockerfile ../../ --load
docker run  -it --privileged --cap-add=SYS_ADMIN --security-opt seccomp=unconfined --cpuset-cpus="0-6" --memory="2g" --memory-swap="2g" --name appframe-container -p 8080:8080 appframe-dev:latest
docker start -ai appframe-container
