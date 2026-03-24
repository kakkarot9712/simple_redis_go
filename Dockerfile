FROM alpine/curl:8.17.0
RUN apk add --no-cache bash git
RUN curl -fsSL https://codecrafters.io/install.sh | bash
WORKDIR /app/redisGo
RUN ls -la
RUN git config --global user.email vikalpgandha9712@gmail.com && \
    git config --global user.name "Vikalp Gandha"
RUN git config --global --add safe.directory /app/redisGo
ENTRYPOINT [ "codecrafters" ]
