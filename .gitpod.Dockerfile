FROM gitpod/workspace-rust

USER gitpod

RUN sudo apt-get update -qq \
 && DEBIAN_FRONTEND=noninteractive sudo apt-get install -y --no-install-recommends \
        clang \
        libclang-dev \
        llvm-dev \
        unixodbc-dev
 && sudo apt-get autoclean && sudo apt-get clean && sudo apt-get -y autoremove \
 && sudo update-ca-certificates \
 && sudo rm -rf /var/lib/apt/lists/*
