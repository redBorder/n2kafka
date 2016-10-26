# n2kafka

[![Build Status](https://travis-ci.org/redBorder/n2kafka.svg?branch=develop)](https://travis-ci.org/redBorder/n2kafka)
[![Coverage Status](https://coveralls.io/repos/github/redBorder/n2kafka/badge.svg?branch=develop)](https://coveralls.io/github/redBorder/n2kafka?branch=develop)

Network to kafka translator. It (currently) support conversion from tcp/udp raw
sockets and HTTP POST to kafka messages, doing message-processing if you need
to.

# Setup
To use it, you only need to do a typical `./configure && make && make install`

# Usage
## Basic usage

In order to send raw tcp messages from port `2056`, to `mymessages` topic, using
`40` threads, you need to use this config file:
```json
{
	"listeners":[
		{"proto":"tcp","port":2056,"num_threads":40}
	],
	"brokers":"localhost",
	"topic":"mymessages"
}
```

And launch `n2kafka` using `./n2kafka <config_file>`. You can also use `udp` and
`http` as proto values. If you want to listen in different ports, you can add as
many listeners as you want.

## Recommended config parameters
You can also use this parameters in config json root to improve n2kafka
behavior:
- `"blacklist":["192.168.101.3"]`, that will ignore requests of this directions
  (useful for load balancers)
- All
  [librdkafka](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).
  options. If a config option starts with `rdkafka.<option>`, `<option>` will be
  passed directly to librdkafka config, so you can use whatever config you need.
  Recommended options are:
  * `"rdkafka.socket.max.fails":"3"`
  * `"rdkafka.socket.keepalive.enable":"true"`
  * `"delivery.report.only.error":"true"`

## Testing

### Unit tests

Run full unit tests suite:

```bash
make tests
```

This will run the full test suite, including Valgrind memory and threads
checking. If no memory and threads checking is required, just run:

```bash
make checks
```

### Integration tests

Integration tests involves some external services that should be running during
testing: Kafka and Zookeeper. By default, integration tests
are disabled due to this requirements and can be enabled through `configure`:

```bash
./configure --enable-integration-tests
```

Docker can be used to easily setup a basic testing environment:

```bash
make setup-tests # Set up a Kafka and Zookeeper in Docker
make teardown-tests # Clean test environment
```
