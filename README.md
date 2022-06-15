# Kafka Streams Example

## Pipe 흐름

`Producer` 에서 메시지 발행 -> `streams-plaintext-input` 토픽에 메시지 저장 -> `Pipe` 에서 Streams에
의해 `streams-pipe-output` 토픽에 메시지 발행 -> `Consumer` 에서 메시지 소비 
