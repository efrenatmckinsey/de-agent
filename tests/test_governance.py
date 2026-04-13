"""Tests for the governance module: classification, lineage."""

from __future__ import annotations

from src.governance.classifier import (
    DataClassification,
    DataClassifier,
    PIIType,
)
from src.governance.lineage import (
    DatasetRef,
    LineageEventType,
    LineageTracker,
    build_dataset_ref_with_facets,
    schema_facet,
)


class TestDataClassifier:
    def test_detect_email(self):
        classifier = DataClassifier()
        records = [
            {"id": 1, "contact": "alice@example.com"},
            {"id": 2, "contact": "bob@test.org"},
            {"id": 3, "contact": "carol@domain.net"},
        ]
        result = classifier.classify_records("email_src", records, {"classification": "public"})
        assert result.classification == DataClassification.CONFIDENTIAL
        assert PIIType.EMAIL in result.pii_fields.values()

    def test_no_pii_stays_public(self):
        classifier = DataClassifier()
        records = [
            {"state": "NY", "population": 19_500_000},
            {"state": "CA", "population": 39_500_000},
        ]
        result = classifier.classify_records("census", records, {"classification": "public"})
        assert result.classification == DataClassification.PUBLIC
        assert len(result.pii_fields) == 0

    def test_ssn_detection(self):
        classifier = DataClassifier()
        records = [
            {"name": "Alice", "ssn": "123-45-6789"},
            {"name": "Bob", "ssn": "987-65-4321"},
        ]
        result = classifier.classify_records("sensitive", records, {"classification": "internal"})
        assert PIIType.SSN in result.pii_fields.values()
        assert result.classification in (
            DataClassification.INTERNAL,
            DataClassification.CONFIDENTIAL,
            DataClassification.RESTRICTED,
        )

    def test_ssn_escalates_from_public(self):
        classifier = DataClassifier()
        records = [
            {"name": "Alice", "ssn": "123-45-6789"},
        ]
        result = classifier.classify_records("sensitive", records, {"classification": "public"})
        assert PIIType.SSN in result.pii_fields.values()
        assert result.classification == DataClassification.CONFIDENTIAL

    def test_classify_empty_records(self):
        classifier = DataClassifier()
        result = classifier.classify_records("empty", [], {"classification": "public"})
        assert result.classification == DataClassification.PUBLIC
        assert result.scanned_rows == 0


class TestLineageTracker:
    def test_start_complete_run(self):
        tracker = LineageTracker()
        run_id = tracker.start_run(
            "ingest:test_source",
            inputs=[DatasetRef(namespace="api", name="test_source")],
            outputs=[DatasetRef(namespace="s3", name="raw/test_source")],
        )
        assert run_id is not None
        assert len(tracker.events) == 1
        assert tracker.events[0].event_type == LineageEventType.START

        event = tracker.complete_run(
            run_id,
            [DatasetRef(namespace="s3", name="raw/test_source")],
        )
        assert event.event_type == LineageEventType.COMPLETE
        assert len(tracker.events) == 2

    def test_fail_run(self):
        tracker = LineageTracker()
        run_id = tracker.start_run(
            "ingest:failing",
            inputs=[DatasetRef(namespace="api", name="bad_source")],
            outputs=[],
        )
        event = tracker.fail_run(run_id, "Connection timeout")
        assert event.event_type == LineageEventType.FAIL
        assert "timeout" in event.facets.get("errorMessage", "").lower()

    def test_lineage_graph(self):
        tracker = LineageTracker()
        run_id = tracker.start_run(
            "ingest:my_source",
            inputs=[DatasetRef(namespace="api", name="my_source")],
            outputs=[DatasetRef(namespace="s3", name="raw/my_source")],
        )
        tracker.complete_run(
            run_id,
            [DatasetRef(namespace="s3", name="raw/my_source")],
        )

        run_id2 = tracker.start_run(
            "transform:my_source",
            inputs=[DatasetRef(namespace="s3", name="raw/my_source")],
            outputs=[DatasetRef(namespace="iceberg", name="curated/my_table")],
        )
        tracker.complete_run(
            run_id2,
            [DatasetRef(namespace="iceberg", name="curated/my_table")],
        )

        graph = tracker.get_lineage_graph("my_source")
        assert "my_source" in graph["source_id"]
        assert len(graph["downstream"]) > 0

    def test_build_dataset_ref_with_facets(self):
        ref = build_dataset_ref_with_facets(
            "s3",
            "raw/test",
            column_names=["id", "name", "age"],
            column_types={"id": "integer", "name": "string"},
            row_count=100,
            null_counts={"name": 5},
            source_id="test_source",
        )
        assert ref.namespace == "s3"
        assert ref.name == "raw/test"
        assert "schema" in ref.facets
        assert len(ref.facets["schema"]["fields"]) == 3


class TestSchemaFacet:
    def test_schema_facet_structure(self):
        facet = schema_facet(["id", "name"], {"id": "integer", "name": "string"})
        assert "fields" in facet
        assert len(facet["fields"]) == 2
        assert facet["fields"][0]["name"] == "id"
        assert facet["fields"][0]["type"] == "integer"
