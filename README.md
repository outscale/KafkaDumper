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
Exporter un ou plusieurs topics

Usage: kafkadumper export [OPTIONS] --topics <TOPICS>

Options:
  -b, --broker <BROKER>              Kafka broker [default: localhost:9092]
  -t, --topics <TOPICS>              Nom des topics à exporter
  -o, --output <OUTPUT>              Nom du fichier d output [default: output]
  -p, --partitions <PARTITIONS>      Partitions spécifiques (format: 1,2,...)
  -n, --max-messages <MAX_MESSAGES>  Nombre maximum de messages à exporter
      --tail                         Mode tail: Récupérer les messages depuis la fin
  -d, --days <DAYS>                  Exporter seulement les N derniers jours
  -g, --group-id <GROUP_ID>          Group ID pour le consumer [default: kafka-dumper]
  -c, --compression <COMPRESSION>    Compression algorithm(level) (https://arrow.apache.org/rust/parquet/basic/enum.Compression.html#variants) [default: uncompressed]
  -s, --split <SPLIT>                Nombre de messages par fichier [default: 0]
  -u, --use-schema-registry <USE_SCHEMA_REGISTRY>
                                     Utiliser le schéma registry pour ajouter le message décodé
      --start-offset <START_OFFSET>  Offset de départ (priorité sur propriétés days et tails)
      --end-offset <END_OFFSET>      Arrêt de consommation par partition
  -h, --help                         Print help
```

Priorité de configuration : Si `start_offset` est défini, `days` et `tail` sont ignorés.

Exemple d'utilisation

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
🚀 Démarrage de l'export des topics: ["dlq-gino-nexus", "dlq-gino-fortinet"]
Mode Tail : Récupération des 100 derniers messages par partition
⠁ [00:00:00] Messages lus: 200                                                                        Tri final : conservation des 100 messages demandés (sur 200 lus)...
  [00:00:00] ✅ 100 messages exportés au total                                                        Découpage des 100 messages en lots de 30...
  Écriture partie 1 (30 messages) -> output-dlq-part-001.parquet
  Écriture partie 2 (30 messages) -> output-dlq-part-002.parquet
  Écriture partie 3 (30 messages) -> output-dlq-part-003.parquet
  Écriture partie 4 (10 messages) -> output-dlq-part-004.parquet
✅ Export terminé avec succès!
```


### Import

```bash
Importer des messages dans un topic

Usage: kafkadumper import [OPTIONS] --input <INPUT>

Options:
  -b, --broker <BROKER>
          Kafka broker [default: localhost:9092]
  -i, --inputs <INPUTS>
          Fichiers d'input
  -T, --target-topic <TARGET_TOPIC>
          Remapper le topic de destination (optionnel)
      --max-message-bytes <MAX_MESSAGE_BYTES>
          Paramètre 'message.max.bytes' (topic et producer) : 1Mib=104857600
      --use-original-topic
          Importer chaque message dans son topic initial
      --use-schema-registry <USE_SCHEMA_REGISTRY>
          Importer les messages à partir du json (parsing dynamique)
  -h, --help
          Print help
```

Exemple d'utilisation

```bash
./kafkadumper import -b kafka.dev-confluent.svc.cluster.local:9092 \
  -i output-dlq-part-001.parquet \
  -i output-dlq-part-002.parquet \
  -T dlq-nexus-fortinet
```

<details><summary>Alternative séparateur</summary><p>

```bash
./kafkadumper import -b kafka.dev-confluent.svc.cluster.local:9092 \
  -i output-dlq-part-001.parquet,output-dlq-part-002.parquet \
  -T dlq-nexus-fortinet
```
</p></details>

<details><summary>Alternative blob</summary><p>
Attention: ici, les quotes sont absolument nécessaire

```bash
./kafkadumper import -b kafka.dev-confluent.svc.cluster.local:9092 \
  -i 'output-dlq-*.parquet' \
  -T dlq-nexus-fortinet
```
</p></details>

<details><summary>Alternative topic par défaut</summary><p>
> Note: Chaque message sera importé dans le topic dans lequel il était initialement.

```bash
./kafkadumper import -b kafka.dev-confluent.svc.cluster.local:9092 \
  -i 'output-dlq-*.parquet' \
  --use-original-topic # optionnel
```
</p></details>

```
🚀 Démarrage de l'import...
📂 Fichiers identifiés : 2
[Topic] Création du topic manquant : 'dlq-nexus-fortinet'
✅ Topic 'dlq-nexus-fortinet' créé avec succès.
  [00:00:05] [████████████████████████████████████████] 30/30 (temps estimé : 0s)   
```

### Inspect

```bash
Analyser un fichier de dump sans importer

Usage: kafkadumper inspect [OPTIONS] --input <INPUT>

Options:
  -i, --input <INPUT>  Fichier d input
  -c, --count <COUNT>  Information mise à jour de vérification [default: 100]
  -v, --verbose        Afficher le détail de chaque message
  -h, --help           Print help
```

Exemple d'utilisation

```bash
./kafkadumper inspect -i output-dlq-part-002.parquet 
```

Output

```
RAPPORT D'INSPECTION : output-dlq-part-002.parquet
--------------------------------------------------
Format        : Apache Parquet
Créé par      : parquet-rs version 57.2.0
Total lignes  : 30
--------------------------------------------------

--------------------------------------------------
Topics trouvés : {"dlq-gino-fortinet", "dlq-gino-nexus"}
 TRI (TIMESTAMP) : + Croissant (Du plus ancien au plus récent)
   Début (Min)    : 28/01/2026 16:39:56 (+01:00) (ts: 1769614796710)
   Fin (Max)      : 28/01/2026 16:45:33 (+01:00) (ts: 1769615133804)
--------------------------------------------------
✅ Vérification réussie : 30 messages valides lus (conforme aux métadonnées Parquet).
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