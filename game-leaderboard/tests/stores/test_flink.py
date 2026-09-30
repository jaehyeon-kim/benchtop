from leaderboard.stores.flink import running


def test_only_this_projects_running_jobs_are_picked():
    """Verify that only jobs named in JOB_NAMES that have not ended are cancelled."""
    jobs = [
        {"jid": "a", "name": "game-top-teams", "state": "RUNNING"},
        {"jid": "b", "name": "game-team-mvps", "state": "CANCELED"},
        {"jid": "c", "name": "someone-elses-job", "state": "RUNNING"},
    ]
    assert running(jobs) == ["a"]
