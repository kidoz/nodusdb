"""Independent PyIceberg writer and a position-delete interoperability fixture."""

import uuid

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.io import load_file_io
from pyiceberg.manifest import (
    DataFile,
    DataFileContent,
    FileFormat,
    ManifestContent,
    ManifestEntry,
    ManifestEntryStatus,
    ManifestWriterV2,
    write_manifest_list,
)
from pyiceberg.table.snapshots import Operation, Snapshot, Summary
from pyiceberg.table.update import (
    AddSnapshotUpdate,
    AssertRefSnapshotId,
    SetSnapshotRefUpdate,
)
from pyiceberg.typedef import Record


class HostCatalog(RestCatalog):
    """Translate only Docker's storage endpoint; preserve vended credentials.

    Lakekeeper must contact MinIO by its Docker service name. Host-side clients
    use its published loopback port. No metadata, schema, or row data is changed.
    The protected method is deliberately tied to the pinned PyIceberg version.
    """

    def __init__(self, uri, endpoint):
        self.host_endpoint = endpoint
        super().__init__("interop", uri=uri, warehouse="interop")

    def _load_file_io(self, properties=None, location=None):
        return load_file_io(
            {
                **self.properties,
                **(properties or {}),
                "s3.endpoint": self.host_endpoint,
            },
            location,
        )


class DeleteManifestWriter(ManifestWriterV2):
    """PyIceberg 0.12 only exposes data-manifest writers; encode delete content.

    Serialization, field IDs, sequence handling, and manifest-list writing still
    come from PyIceberg. Its reader must independently apply this delete before
    the NodusDB rejection check can pass.
    """

    def content(self):
        return ManifestContent.DELETES

    @property
    def _meta(self):
        return {**super()._meta, "content": "deletes"}


def add_position_delete(catalog, table):
    """Commit a real v2 position-delete snapshot removing row zero of one file."""
    parent = table.current_snapshot()
    files = list(table.scan().plan_files())
    assert len(files) == 1, "position-delete fixture requires one data file"
    snapshot_id = uuid.uuid4().int & ((1 << 63) - 1)
    sequence = table.metadata.last_sequence_number + 1
    prefix = table.location() + "/metadata/" + str(uuid.uuid4())
    delete_path = table.location() + "/data/" + str(uuid.uuid4()) + "-delete.parquet"
    schema = pa.schema(
        [
            pa.field(
                "file_path",
                pa.string(),
                nullable=False,
                metadata={b"PARQUET:field_id": b"2147483546"},
            ),
            pa.field(
                "pos",
                pa.int64(),
                nullable=False,
                metadata={b"PARQUET:field_id": b"2147483545"},
            ),
        ]
    )
    data = pa.Table.from_pylist(
        [{"file_path": files[0].file.file_path, "pos": 0}], schema=schema
    )
    with table.io.new_output(delete_path).create() as output:
        pq.write_table(data, output)
    delete = DataFile.from_args(
        content=DataFileContent.POSITION_DELETES,
        file_path=delete_path,
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=1,
        file_size_in_bytes=len(table.io.new_input(delete_path)),
    )
    with DeleteManifestWriter(
        table.spec(),
        table.schema(),
        table.io.new_output(prefix + ".avro"),
        snapshot_id,
        "null",
    ) as writer:
        writer.add_entry(
            ManifestEntry.from_args(
                status=ManifestEntryStatus.ADDED,
                snapshot_id=snapshot_id,
                sequence_number=None,
                file_sequence_number=None,
                data_file=delete,
            )
        )
    manifests = [*parent.manifests(table.io), writer.to_manifest_file()]
    manifest_list = prefix + "-list.avro"
    with write_manifest_list(
        2,
        table.io.new_output(manifest_list),
        snapshot_id,
        parent.snapshot_id,
        sequence,
        "null",
    ) as writer:
        writer.add_manifests(manifests)
    snapshot = Snapshot(
        snapshot_id=snapshot_id,
        parent_snapshot_id=parent.snapshot_id,
        sequence_number=sequence,
        schema_id=table.schema().schema_id,
        manifest_list=manifest_list,
        summary=Summary(
            operation=Operation.DELETE,
            **{
                "added-position-delete-files": "1",
                "added-position-deletes": "1",
                "total-position-deletes": "1",
                "total-delete-files": "1",
                "total-data-files": "1",
                "total-records": str(files[0].file.record_count),
            },
        ),
    )
    catalog.commit_table(
        table,
        requirements=(AssertRefSnapshotId(ref="main", snapshot_id=parent.snapshot_id),),
        updates=(
            AddSnapshotUpdate(snapshot=snapshot),
            SetSnapshotRefUpdate(
                ref_name="main", type="branch", snapshot_id=snapshot_id
            ),
        ),
    )
    return catalog.load_table(table.name())
