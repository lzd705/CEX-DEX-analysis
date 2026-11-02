# Research hub verification — 2026-11-02

## Publication decision

Publish Design A: one default-branch research entry with version-isolated
implementations. The user explicitly selected publication after receiving the
legacy Foundry test status below. Production services, timers, and data are
unchanged. Existing research branches and the originating dirty checkout are
preserved.

## Source identity

| Component | Pinned source |
| --- | --- |
| Root application / Foundry | `83e66079f679c8133c6a6415238d27377d42293b` |
| V3 exact implementation | `401eaad12965560ffd67e60caad60d2a267e206d` |
| SHIB V2/V2 research | `b16ee24d79806a0de655b28caa3ce6f9d248ba1e` |
| forge-std Gitlink | `620536fa5277db4e3fd46772d5cbc1ea0696fb43` |

All **857 imported source blobs** preserve their original bytes and executable
modes. The Gitlink is retained. [Source inventory](../research/sources.json)
separately declares new publication files. Original READMEs remain available
under their declared relocation paths. The 18 fingerprinted changed/untracked
files in the originating checkout were unchanged.

## Verification results

| Verification | Result |
| --- | --- |
| Root hub, source, documentation, replay, experiment, quantity and CEX-depth regressions | 171 tests passed |
| Isolated V3 module (`test_uniswap_v3_*.py`) | 76 tests passed |
| Isolated SHIB module (`test_shib_v2_research.py`) | 80 tests passed |
| Existing historical/current demo tests | 21 tests passed |
| Bilingual documentation checker | 62 local links; 2 matching shell blocks |
| GitHub Markdown rendering | Both languages rendered; tables, math, website and language links checked |
| Source provenance | 857 blobs, 1 Gitlink, and 362 publication additions verified |
| Published experiment integrity inventory | 331 entries |
| Privacy scan of original dataset | 329 files; zero credential or machine-path findings |

Counts describe the named suites and are not added together as a distinct-test
total. The root command is:

The final acceptance run used local-loopback permission for HTTP redirect and
demo tests. A restricted rerun initially failed to bind 127.0.0.1; the same
unchanged test suite passed with that permission available.

```bash
python3 -B -m unittest tests.test_research_sources tests.test_run_research tests.test_research_docs tests.test_replay_uni_cex_v3 tests.test_uni_cex_v3_experiment tests.test_analyze_uni_cex_v3_experiment tests.test_uni_experiments tests.test_route_quantity tests.test_fetch_cex_depth -q
```

The replay regression denies socket connections and verifies unchanged capture
and reference-result hashes. The fixed dispatcher tests subprocess import
isolation, hostile ambient Python paths, and child exit-code propagation.

## Reproduced market experiment

[Published evidence and reproduction guide](../experiments/uni_cex_v3_20261102/README.md).

- Observation: 2026-11-02, 13:10:45–13:20:41 HKT.
- 60 polls; 51 accepted distinct block states; 9 duplicate-head polls.
- 510 DEX quotes; 21,420 fee/Gas scenario rows; 13,770 delay scenario rows.
- Zero positive fee/Gas cash-flow rows in this retained window.
- All four regenerated result files are byte-identical to the published references.
- SHIB snapshot rebuild is byte-identical, SHA-256:
  `75768bb4dd0d87a738fba2a18440571b81e708a020d4e4967fb299f61ad4c7c6`.

| Result file | SHA-256 |
| --- | --- |
| fee_gas_timeseries.csv | `2bc42848e65939037e23468e9c8e2f92570af9daf3b47923b4faeca35493386c` |
| delay_pairs.csv | `ac6cb6ffbe7cddd6df26f839fa627adeabea0fc830c0d3c33647d9d557c15f50` |
| network_gas_scenarios.csv | `3263221e494541fcd7bbde0e323c60c8f9a30cf5e78d1d2530d2d6afa8431a8e` |
| summary.json | `416c9a05a44caa3c0a21d929dee115b944ce37ff9f1cb53be1ac4ed8ff12c567` |

## Independent review and corrections

One independent whole-branch review checked source identities, output-path
protection, offline reproducibility, documentation, and publication contents.

1. Contradictory manifest identities could pass the original capture verifier.
   The verifier now checks exact schema, chain, pool, quoter, fee tier and
   eligibility declarations, including their types, plus retained
   `eth_chainId` evidence. Incorrect or missing identity evidence is rejected
   before replay output is created.
2. The initial additions inventory omitted later navigation and helper files.
   The inventory now covers publication additions; the verifier rejects
   missing, duplicate and unsafe declared paths.

Four added regression methods produced eight failing assertions before the
fixes. The review fix pass then passed the affected tests and the 171-test
root acceptance suite. Original raw responses and reference results were not
changed. The analyzer's original and published hashes are both recorded in the
[evidence index](../experiments/uni_cex_v3_20261102/evidence-index.json).

## Legacy full-suite status

An additional `python3 -B -m unittest discover -s tests -v` run was attempted.
It recorded **65 ERROR entries** before being stopped after more than eleven
minutes; it did not produce a final full-suite summary.

One error was reproduced independently:
`HistoricalCompleteBundleTests.test_complete_pointer_cas_rejects_intervening_writer`
raises `HistoricalFoundryToolchainError: forge_std_tree_invalid`.
The pinned forge-std/toolchain dependency is not initialized in this checkout.
The other 64 errors have not each been individually diagnosed.

The expensive
`HistoricalFoundryScanTask4bReplayTests.test_slice5_precall_manifest_drift_delivers_zero_events`
case was run separately and **passed in 195.365 seconds**.
It is not classified as an infinite hang.

These results are recorded separately from the passing offline hub/module
acceptance tests. The user selected publication with this legacy status
documented; legacy Foundry environment initialization and full-suite rerun
remain a follow-up task.

### Recorded ERROR test identifiers

```text
test_historical_complete_bundle.HistoricalCompleteBundleTests.test_complete_pointer_cas_rejects_intervening_writer
test_historical_complete_bundle.HistoricalCompleteBundleTests.test_complete_pointer_publication_is_exact_one_shot_cas
test_historical_complete_bundle.HistoricalCompleteBundleTests.test_complete_pointer_rolls_back_when_post_install_evidence_changes
test_historical_complete_bundle.HistoricalCompleteBundleTests.test_evidence_join_rejects_rehashed_noncanonical_scenario_facts
test_historical_complete_bundle.HistoricalCompleteBundleTests.test_old_bundle_validates_after_historical_core_latest_advances
test_historical_complete_bundle.HistoricalCompleteBundleTests.test_post_rename_public_validation_failure_rolls_back_and_retries
test_historical_complete_bundle.HistoricalCompleteBundleTests.test_pre_rename_validation_failure_cleans_stage_and_retry_succeeds
test_historical_complete_bundle.HistoricalCompleteBundleTests.test_stage_and_validate_six_file_bundle_without_moving_historical_latest
test_historical_complete_bundle.HistoricalCompleteBundleTests.test_staged_core_context_supports_full_dry_run_bundle_without_pointers
test_historical_complete_bundle.HistoricalCompleteBundleTests.test_writer_rejects_rehashed_block_derivatives_outside_sealed_input
test_historical_complete_bundle.HistoricalCompleteBundleTests.test_writer_rejects_rehashed_selected_block_outside_sealed_context
test_historical_complete_bundle.HistoricalCompleteBundleTests.test_writer_rejects_rehashed_source_facts_outside_sealed_input
test_historical_complete_bundle.HistoricalCompleteBundleTests.test_writer_rejects_replaced_per_scenario_fact_oracle
test_historical_complete_capabilities.HistoricalCompleteCapabilityTests.test_published_cost_proof_is_bound_to_one_validated_view
test_historical_complete_capabilities.HistoricalCompleteCapabilityTests.test_repeated_stage_is_byte_identical_and_reuses_one_replay
test_historical_complete_capabilities.HistoricalCompleteCapabilityTests.test_view_close_and_gc_release_the_owned_immutable_context
test_historical_digest_lineage.HistoricalDigestLineageTests.test_writer_rejects_replaced_overlay_and_scenario_set_digest_oracle
test_historical_final_context_reread.HistoricalFinalContextRereadTests.test_stage_rejects_same_byte_raw_inode_replacement_after_final_validation
test_historical_foundry_anvil.HistoricalFoundryClosedRevertTests.test_exact_outer_and_inner_revert_axes_close_only_the_allowlisted_case
test_historical_foundry_anvil.HistoricalFoundryClosedRevertTests.test_status_zero_quartet_freezes_only_exact_atomic_rollback
test_historical_foundry_anvil.HistoricalFoundryOfflineRepeatTests.test_two_status_one_repeats_and_fresh_status_zero_use_real_anvil
test_historical_foundry_anvil.HistoricalFoundryProcessLeaseTests.test_identity_failure_never_resumes_and_emergency_lease_reaps
test_historical_foundry_anvil.HistoricalFoundryProcessLeaseTests.test_material_fd_close_side_effect_is_not_retried_by_number
test_historical_foundry_anvil.HistoricalFoundryProcessLeaseTests.test_pending_authority_precedes_materialization_and_owns_exact_pid_cell
test_historical_foundry_anvil.HistoricalFoundryProcessLeaseTests.test_pending_emergency_lease_is_registered_before_spawn_call
test_historical_foundry_anvil.HistoricalFoundryProcessLeaseTests.test_private_materialization_rejects_symlink_and_post_spawn_substitution
test_historical_foundry_anvil.HistoricalFoundryProcessLeaseTests.test_raced_foreign_mkdir_eexist_is_never_adopted_or_deleted
test_historical_foundry_anvil.HistoricalFoundryProcessLeaseTests.test_spawn_uses_held_fd_and_final_lease_precedes_resume
test_historical_foundry_anvil.HistoricalFoundryProcessLeaseTests.test_work_directory_substitution_is_retained_without_cross_delete
test_historical_foundry_contracts.HistoricalFoundryTrackedLoaderTests.test_build_rejects_config_ancestor_drift_during_toolchain_invocation
test_historical_foundry_contracts.HistoricalFoundryTrackedLoaderTests.test_build_rejects_config_byte_drift_during_toolchain_invocation
test_historical_foundry_contracts.HistoricalFoundryTrackedLoaderTests.test_build_rejects_config_inode_drift_during_toolchain_invocation
test_historical_foundry_contracts.HistoricalFoundryTrackedLoaderTests.test_real_build_returns_sealed_hash_bound_artifact
test_historical_foundry_replay.HistoricalOpportunityBridgeTests.test_apparent_issuer_symbols_cannot_mint_arbitrary_capabilities
test_historical_foundry_replay.HistoricalOpportunityBridgeTests.test_cashflow_rejects_each_retained_execution_projection_attack
test_historical_foundry_replay.HistoricalOpportunityBridgeTests.test_compact_evidence_rejects_fully_rebound_final_output_attack
test_historical_foundry_replay.HistoricalOpportunityBridgeTests.test_compact_evidence_rejects_fully_rebound_proof_row_and_gas_attacks
test_historical_foundry_replay.HistoricalOpportunityBridgeTests.test_compact_evidence_rejects_self_rehashed_malformed_inventory
test_historical_foundry_replay.HistoricalOpportunityBridgeTests.test_equivalent_staged_and_committed_capabilities_produce_identical_projection
test_historical_foundry_replay.HistoricalOpportunityBridgeTests.test_historical_opportunity_binds_the_exact_core_manifest
test_historical_foundry_replay.HistoricalOpportunityBridgeTests.test_installed_scenario_issuer_captures_exact_material_validator
test_historical_foundry_replay.HistoricalOpportunityBridgeTests.test_publication_facts_bind_ordered_overlays_and_raw_sources
test_historical_foundry_replay.HistoricalOpportunityBridgeTests.test_publication_facts_reject_cross_block_and_rebound_source_size
test_historical_foundry_replay.HistoricalOpportunityBridgeTests.test_publication_rejects_stale_capability_before_matrix_and_serialization
test_historical_foundry_replay.HistoricalOpportunityBridgeTests.test_publication_rejects_transplanted_scenario_capability_before_pure_bridge
test_historical_foundry_replay.HistoricalOpportunityBridgeTests.test_pure_bridge_performs_no_io_clock_rpc_or_subprocess
test_historical_foundry_replay.HistoricalOpportunityBridgeTests.test_replay_rejects_canonical_projection_bytes_before_arithmetic
test_historical_foundry_replay.HistoricalOpportunityBridgeTests.test_same_capability_produces_byte_identical_projection
test_historical_foundry_replay.HistoricalOpportunityBridgeTests.test_scenario_capability_retains_p50_calls_and_post_pair_state
test_historical_foundry_replay.HistoricalOpportunityBridgeTests.test_scenario_projection_retains_independent_economics_authority
test_historical_foundry_replay.HistoricalOpportunityBridgeTests.test_ten_scenarios_produce_ninety_costs_and_compact_evidence
test_historical_foundry_scan.HistoricalCandidateSelectionTests.test_closed_publication_lease_cannot_be_consumed_or_reissued
test_historical_foundry_scan.HistoricalCandidateSelectionTests.test_descending_controller_rejects_out_of_order_ledger_prefix
test_historical_foundry_scan.HistoricalCandidateSelectionTests.test_existing_successful_quartet_is_never_reissued_or_overwritten
test_historical_foundry_scan.HistoricalCandidateSelectionTests.test_final_no_replace_failure_rolls_back_without_formal_visibility
test_historical_foundry_scan.HistoricalCandidateSelectionTests.test_final_run_manifest_is_last_logical_id_mapped_and_reopenable
test_historical_foundry_scan.HistoricalCandidateSelectionTests.test_newest_full_ten_status_one_positive_is_selected_exactly_once
test_historical_foundry_scan.HistoricalCandidateSelectionTests.test_nonpublishable_newer_candidate_continues_to_older_winner
test_historical_foundry_scan.HistoricalCandidateSelectionTests.test_only_nonpublishable_complete_window_finalizes_closed_with_no_typed
test_historical_foundry_scan.HistoricalCandidateSelectionTests.test_post_rename_parent_fsync_failure_retries_forward_without_rewrite
test_historical_foundry_scan.HistoricalCandidateSelectionTests.test_private_next_action_issues_exact_task6_scenario_and_is_one_shot
test_historical_foundry_scan.HistoricalCandidateSelectionTests.test_publication_lease_is_live_finalizer_only_and_one_shot
test_historical_foundry_scan.HistoricalCandidateSelectionTests.test_selected_typed_members_are_canonical_market_keyed_and_sealed
test_historical_foundry_scan.HistoricalCandidateSelectionTests.test_storage_independently_rejects_candidate_typed_and_selection_tamper
test_historical_foundry_scan.HistoricalCandidateSelectionTests.test_unknown_replay_failure_is_unresolved_terminal_not_no_opportunity
```

## Implementation decisions and review dispositions

| Decision | Reason | Trade-off / remaining work |
| --- | --- | --- |
| Separate managed integration worktree | Preserve originating dirty checkout | A separate checkout must be maintained |
| Approved writes in the managed worktree | Initial filesystem write roots did not include it | Writes require the correct workspace permissions |
| Preserve inherited source whitespace | Keep pinned-source byte identity | Older formatting remains; newly authored diff is checked separately |
| Mechanically test links/commands; manually inspect prose | Avoid brittle tests of editorial wording | Prose accuracy requires human review when methods change |
| Treat incomplete additions provenance as Important | Complete provenance is part of Design A | Inventory must be updated with future publication files |
| Publish offline hub while recording legacy toolchain errors | Explicit user choice after test-status disclosure | Full legacy suite is not established as passing |
| Retain private diagnostic artifacts locally | Preserve the user's requested verification evidence | Ignored local scratch uses disk space; none is published |

Review items outside direct reviewer validation are resolved as follows:
production is unchanged and remains outside this publication; GitHub source
availability is checked after the normal non-force push; the original checkout
is checked by recorded hashes; inherited implementations receive source-identity
and named-suite checks, not a new exhaustive audit. The older two-pool canary's
20 raw scenario receipts were not located and their availability is recorded in
the evidence index rather than inferred from configuration. No minor findings
remain deferred from the final review.
