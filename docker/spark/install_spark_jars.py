"""Bake the Spark connector jars into the installed pyspark's jars/ directory.

Runs at image build time (runtime and airflow images) so Spark never downloads
anything at runtime (no --packages / Ivy). Versions are derived from the
installed pyspark: the Kafka connector matches pyspark exactly, and
kafka-clients / commons-pool2 come from the matching spark-parent POM.
Every jar is verified against Maven Central's SHA-1 checksum.
"""

from __future__ import annotations

import hashlib
import os
import re
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import pyspark

MAVEN = os.getenv("MAVEN_REPO", "https://repo1.maven.org/maven2")
PG_JDBC_VERSION = os.environ["POSTGRES_JDBC_VERSION"]


def fetch(url: str, attempts: int = 5) -> bytes:
    for attempt in range(1, attempts + 1):
        try:
            with urllib.request.urlopen(url, timeout=120) as resp:
                return resp.read()
        except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
            if attempt == attempts:
                raise
            print(f"  retry {attempt}/{attempts - 1} for {url}: {exc}")
            time.sleep(3 * attempt)
    raise AssertionError("unreachable")


def pom_property(pom: str, name: str) -> str:
    match = re.search(rf"<{re.escape(name)}>([^<]+)</{re.escape(name)}>", pom)
    if not match:
        sys.exit(f"property {name} not found in spark-parent POM")
    return match.group(1).strip()


def download(group: str, artifact: str, version: str, dest_dir: Path) -> Path:
    base = f"{MAVEN}/{group.replace('.', '/')}/{artifact}/{version}/{artifact}-{version}.jar"
    data = fetch(base)
    expected = fetch(base + ".sha1").decode().split()[0].strip()
    actual = hashlib.sha1(data).hexdigest()
    if actual != expected:
        sys.exit(f"checksum mismatch for {base}: {actual} != {expected}")
    dest = dest_dir / f"{artifact}-{version}.jar"
    dest.write_bytes(data)
    return dest


def main() -> None:
    spark_version = pyspark.__version__
    jars_dir = Path(pyspark.__file__).parent / "jars"
    core = sorted(jars_dir.glob("spark-core_*.jar"))
    if not core:
        sys.exit(f"no spark-core jar in {jars_dir}")
    scala = re.match(r"spark-core_(\d+\.\d+)-", core[0].name).group(1)

    parent = fetch(
        f"{MAVEN}/org/apache/spark/spark-parent_{scala}/{spark_version}/"
        f"spark-parent_{scala}-{spark_version}.pom"
    ).decode()
    kafka_version = pom_property(parent, "kafka.version")
    pool2_version = pom_property(parent, "commons-pool2.version")

    wanted = [
        ("org.apache.spark", f"spark-sql-kafka-0-10_{scala}", spark_version),
        ("org.apache.spark", f"spark-token-provider-kafka-0-10_{scala}", spark_version),
        ("org.apache.kafka", "kafka-clients", kafka_version),
        ("org.apache.commons", "commons-pool2", pool2_version),
        ("org.postgresql", "postgresql", PG_JDBC_VERSION),
    ]
    print(f"pyspark {spark_version} (scala {scala}) -> {jars_dir}")
    for group, artifact, version in wanted:
        existing = list(jars_dir.glob(f"{artifact}-*.jar"))
        if existing:
            sys.exit(f"{artifact} already present in pyspark jars: {existing}")
        path = download(group, artifact, version, jars_dir)
        print(f"  installed {path.name} ({path.stat().st_size // 1024} KiB)")


if __name__ == "__main__":
    main()
