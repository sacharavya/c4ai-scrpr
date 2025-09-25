from app.orchestrator.checkpoint import JobCheckpoint, clear_checkpoint, load_checkpoint, save_checkpoint


def test_checkpoint_roundtrip(tmp_path):
    checkpoint = JobCheckpoint(
        job_id="abc",
        url_cursor="https://example.org/page",
        page_idx=2,
        discovered_urls_hash="deadbeef",
    )
    save_checkpoint(tmp_path, "events-run", checkpoint)
    restored = load_checkpoint(tmp_path, "events-run")
    assert restored == checkpoint
    clear_checkpoint(tmp_path, "events-run")
    assert load_checkpoint(tmp_path, "events-run") is None
