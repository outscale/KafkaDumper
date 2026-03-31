## **KafkaDumper**

[![Project Stage](https://docs.outscale.com/fr/userguide/_images/Project-Sandbox-yellow.svg)](https://docs.outscale.com/en/userguide/Open-Source-Projects.html) [![](https://dcbadge.limes.pink/api/server/HUVtY5gT6s?style=flat&theme=default-inverted)](https://discord.gg/HUVtY5gT6s)

---

## 🌐 Links

- Documentation: <https://docs.outscale.com/en/>
- Project website: <https://github.com/outscale/KafkaDumper>
- Join our community on [Discord](https://discord.gg/HUVtY5gT6s)
- Related tools or community: <https://kafka.apache.org/>

---

## 📄 Table of Contents

- [Overview](#-overview)
- [Requirements](#-requirements)
- [Installation](#-installation)
- [Usage](#-usage)
- [License](#-license)
- [Contributing](#-contributing)

---

## 🧭 Overview

KafkaDumper is a command-line tool that allows you to create a snapshot of Kafka topics at a specific point in time. You can specify various import parameters. It is also possible to export messages as JSON from the Schema Registry (using dynamic Protobuf deserialization and serialization). Backups are saved in [Apache Parquet](https://parquet.apache.org/) format.

Key features:
- Exporting/Importing Kafka messages on specific topics
- Dynamic Protobuf deserialization/serialization based on the schema registry (useful for schemas of different versions)

---

## ✅ Requirements

- Access to a Kafka server

---

## ⚙ Installation

### Option 1: Download from Releases

Download the latest binary from the [Releases page](https://github.com/outscale/KafkaDumper/releases).

### Option 2: Homebrew

```bash
brew tap outscale/tap
brew install outscale/tap/kafkadumper
```

### Option 3: Install from source

```bash
git clone https://github.com/outscale/KafkaDumper.git
cd KafkaDumper
cargo build --release

# or

cargo run -- import ...
```

---

## 🚀 Usage

### Export

```bash
Export one or more topics

Usage: kafkadumper export [OPTIONS] --topics <TOPICS>

Options:
  -b, --broker <BROKER>
          Kafka broker [default: localhost:9092]
  -t, --topics <TOPICS>
          Names of topics to export
  -o, --output <OUTPUT>
          Output file name
  -p, --partitions <PARTITIONS>
          Specific partitions (format: 1, 2, ...)
  -n, --max-messages <MAX_MESSAGES>
          Maximum number of messages to export
      --tail
          Tail mode: Retrieve messages from the end
  -d, --days <DAYS>
          Export only the last N days
  -g, --group-id <GROUP_ID>
          Group ID for the consumer [default: kafka-dumper]
  -c, --compression <COMPRESSION>
          Compression algorithm(level) (https://arrow.apache.org/rust/parquet/basic/enum.Compression.html#variants) [default: uncompressed]
  -s, --split <SPLIT>
          Number of messages per file [default: 0]
  -u, --use-schema-registry <USE_SCHEMA_REGISTRY>
          Use the registry schema to add the decoded message
      --start-offset <START_OFFSET>
          Initial offset (takes precedence over the `days` and `tails` properties)
      --end-offset <END_OFFSET>
          Stop consumption by partition
  -h, --help
          Print help
```

Configuration priority: If `start_offset` is set, `days` and `tail` are ignored.

Example of use

```bash
./kafkadumper export -b kafka.prod-confluent.svc.cluster.local:9092 \
  -u http://schemaregistry.prod-confluent.svc.cluster.local:8081 \
  -t dlq-gino-nexus \
  -t dlq-gino-fortinet \
  -n 100 \
  -s 30 \
  -c "GZIP(6)" \
  -o output-dlq \
  --tail
```

Output

```
🚀 Starting export of topics: ["dlq-gino-nexus", "dlq-gino-fortinet"]
Tail mode: Fetching the last 100 messages per partition
⠁ [00:00:00] Messages read: 200                                                                        Tri final : conservation des 100 messages demandés (sur 200 lus)...
  [00:00:00] ✅ 100 messages exported in total                                                        Découpage des 100 messages en lots de 30...
  Writing part 1 (30 messages) -> output-dlq-part-001.parquet
  Writing part 2 (30 messages) -> output-dlq-part-002.parquet
  Writing part 3 (30 messages) -> output-dlq-part-003.parquet
  Writing part 4 (10 messages) -> output-dlq-part-004.parquet
✅ Export completed successfully!
```


### Import

```bash
Import messages into a topic

Usage: kafkadumper import [OPTIONS] --inputs <INPUTS>

Options:
  -b, --broker <BROKER>
          Kafka broker [default: localhost:9092]
  -i, --inputs <INPUTS>
          Input files
  -T, --target-topic <TARGET_TOPIC>
          Remap the destination topic
      --max-message-bytes <MAX_MESSAGE_BYTES>
          'message.max.bytes' parameter (topic and producer): 1 MiB = 10,485,760 bytes
      --use-original-topic
          Move each message back to its original thread
      --use-schema-registry <USE_SCHEMA_REGISTRY>
          Import messages from JSON (dynamic parsing)
  -h, --help
          Print help
```

Example of use

```bash
./kafkadumper import -b kafka.dev-confluent.svc.cluster.local:9092 \
  -i output-dlq-part-001.parquet \
  -i output-dlq-part-002.parquet \
  -T dlq-nexus-fortinet
```

<details><summary>Alternative separator</summary><p>

```bash
./kafkadumper import -b kafka.dev-confluent.svc.cluster.local:9092 \
  -i output-dlq-part-001.parquet,output-dlq-part-002.parquet \
  -T dlq-nexus-fortinet
```
</p></details>

<details><summary>Alternative blob</summary><p>
Note: The quotation marks are absolutely necessary here

```bash
./kafkadumper import -b kafka.dev-confluent.svc.cluster.local:9092 \
  -i 'output-dlq-*.parquet' \
  -T dlq-nexus-fortinet
```
</p></details>

<details><summary>Alternative default topic</summary><p>
> Note: Each message will be imported into the thread where it was originally posted.

```bash
./kafkadumper import -b kafka.dev-confluent.svc.cluster.local:9092 \
  -i 'output-dlq-*.parquet' \
  --use-original-topic # optional
```
</p></details>

```
🚀 Starting import...
📂 Files found : 2
[Topic] Creating missing topic : 'dlq-nexus-fortinet'
✅ Topic 'dlq-nexus-fortinet' created successfully.
  [00:00:05] [████████████████████████████████████████] 30/30 (estimated time : 0s)   
```

### Inspect

```bash
Analyze a dump file without importing it

Usage: kafkadumper inspect [OPTIONS] --input <INPUT>

Options:
  -i, --input <INPUT>  Input file
  -c, --count <COUNT>  Updated verification information [default: 100]
  -v, --verbose        View the details of each message
  -h, --help           Print help
```

Example of use

```bash
./kafkadumper inspect -i output-dlq-part-002.parquet 
```

Output

```
INSPECTION REPORT : output-dlq-part-002.parquet
--------------------------------------------------
Format        : Apache Parquet
Created by    : parquet-rs version 57.2.0
Total rows    : 30
--------------------------------------------------

--------------------------------------------------
Topics found : {"dlq-gino-fortinet", "dlq-gino-nexus"}
 SORT (TIMESTAMP) : + Ascending (From oldest to newest)
   Start (Min)    : 28/01/2026 16:39:56 (+01:00) (ts: 1769614796710)
   End (Max)      : 28/01/2026 16:45:33 (+01:00) (ts: 1769615133804)
--------------------------------------------------
✅ Verification successful : 30 valid messages read (compliant with Parquet metadata).
```

---

## 📜 License

**KafkaDumper** is released under the BSD 3-Clause license.

© 2026 Outscale SAS

See [LICENSE](./LICENSE) for full details.

---

## 🤝 Contributing

We welcome contributions!

Please read our [Contributing Guidelines](CONTRIBUTING.md) and [Code of Conduct](CODE_OF_CONDUCT.md) before submitting a pull request.