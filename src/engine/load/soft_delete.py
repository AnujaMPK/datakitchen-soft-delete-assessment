"""``soft_delete`` load mode: Delta MERGE with insert / update / soft delete."""

from __future__ import annotations

from typing import TYPE_CHECKING

from delta.tables import DeltaTable
from pyspark.sql.column import Column

from engine.errors import LoadError

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class SoftDeleteLoader:
    """Synchronises a Delta target with a source DataFrame without physical deletes."""

    def __init__(self, primary_keys: list[str]) -> None:
        if not primary_keys:
            raise LoadError("SoftDeleteLoader requires at least one primary key.")
        self.primary_keys = primary_keys

    def _merge_condition(self) -> str:
        return " AND ".join(f"target.{k} = source.{k}" for k in self.primary_keys)

    def run(self, source: DataFrame, target_path: str) -> None:
        """Run the MERGE against the Delta table at ``target_path``."""
        spark = source.sparkSession
        target = DeltaTable.forPath(spark, target_path)

        update_set: dict[str, str | Column] = {
            column: f"source.{column}"
            for column in source.columns
            if column not in {"is_active", "deleted_at"}
        }
        update_set["is_active"] = "true"
        update_set["deleted_at"] = "null"

        insert_set = update_set.copy()

        (
            target.alias("target")
            .merge(source.alias("source"), self._merge_condition())
            .whenMatchedUpdate(set=update_set)
            .whenNotMatchedInsert(values=insert_set)
            .whenNotMatchedBySourceUpdate(
                condition="target.is_active = true",
                set={
                    "is_active": "false",
                    "deleted_at": "current_timestamp()",
                },
            )
            .execute()
        )
