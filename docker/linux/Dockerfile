FROM archlinux:multilib-devel-20250921.0.423275

RUN pacman -Syu --noconfirm \
    base-devel \
    liburing \
    git \
    cmake \
    gcc \
    make \
    ninja \
    boost \
    boost-libs

RUN useradd -m builder && \
    echo 'builder ALL=(ALL) NOPASSWD: ALL' >> /etc/sudoers

USER builder
RUN git clone https://aur.archlinux.org/wrk.git /tmp/wrk && \
    cd /tmp/wrk && \
    makepkg -si --noconfirm && \
    rm -rf /tmp/wrk
USER root
WORKDIR /app
COPY . /app

RUN ["cmake", "-S", "/app", "-B", "/app/docker-build", "-G", "Ninja Multi-Config", "-DCMAKE_BUILD_TYPE=Release"]

RUN ["cmake", "--build", "/app/docker-build", "--config", "Release"]

# RUN ["/usr/bin/systemd-run", "-G", "-p", "MemoryMax=1G", "-p", "MemorySwapMax=100M", "/app/docker-build/bin/Release/iocontext_http_server", "&"]

# RUN ["/usr/bin/wrk", "-t", "7", "-c", "100", "-d", "30s", "http://localhost:8080/"]