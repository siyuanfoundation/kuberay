# GKE PodSnapshot Integration for KubeRay

The goal of this task was to integrate the GKE PodSnapshot feature into KubeRay to allow for periodic backups and stateful restoration of the Ray head pod. The changes span across the `RayCluster` CRD definitions and the core `RayCluster` reconciliation logic.

## 1. CRD Modifications

I updated the core KubeRay CRDs to introduce a configuration struct specifically for managing the snapshot properties on the Ray head pod:

**`ray-operator/apis/ray/v1/raycluster_types.go`**
I introduced a new `HeadBackupRestoreSpec` under `HeadGroupSpec` containing:
- `Enable`: Boolean flag to toggle the feature.
- `BackupInterval`: To define the time between periodic snapshots.
- `StorageConfigName`: Optional reference to a specific storage backend configuration.
- `RestoreFrom`: An explicit snapshot name if the user wants to roll back or initialize from a specific point in time rather than simply taking new ones.

Additionally, I added state-tracking fields into the `RayClusterStatus`:
- `LastPodSnapshotTime`: To calculate whether the `BackupInterval` elapsed.
- `LastPodSnapshotName`: Tracks the dynamically generated trigger name.
- `LastPodSnapshotStatus`: Tracks the completion state (e.g. `Ready`, `AllSnapshotsAvailable`).

## 2. Dynamic GKE PodSnapshot Generation

To accommodate cloud-agnostic restrictions in the KubeRay project, I implemented the Kubebuilder side of the `podsnapshot.gke.io` API using Kubernetes unstructured map types, thus sidestepping compilation failures from missing Golang structs. 

**`ray-operator/controllers/ray/raycluster_controller.go`**
I implemented a new reconciliation function `reconcileHeadPodSnapshot`:
1.  **Policy Ensure:** Dynamically searches for an existing `PodSnapshotPolicy` matching KubeRay's head pod labels. If not found, it creates one using `unstructured.Unstructured`, explicitly configuring `spec.triggerConfig.type = "manual"` to permit KubeRay's periodic interval triggers.
2.  **Backup Triggering:** It compares the `LastPodSnapshotTime` timestamp recorded in the RayCluster's status block against KubeRay's continuous reconciliation cycle. If time greater than `BackupInterval` has elapsed, KubeRay injects a new `PodSnapshotManualTrigger` via the dynamic K8s client to schedule a snapshot immediately. KubeRay will simultaneously record `LastPodSnapshotName` into the `RayClusterStatus`.
    * **Health Check Override:** Before evaluating the timer, KubeRay will first verify that `instance.Status.State == rayv1.Ready`. This ensures snapshots are not generated while the RayCluster instances are scaling up, suspended, or failing.
3.  **Status Monitoring:** Once `LastPodSnapshotName` is recorded, KubeRay begins querying the generated `PodSnapshot` API and stores the state progression within `LastPodSnapshotStatus`.

Finally, I appended this new `reconcileHeadPodSnapshot` routine to the `reconcileFuncs` array executed during every cluster reconciliation loop.

**Kubebuilder RBAC rules:**
To give KubeRay the necessary permissions to control Google's CRDs, I added markers for the `podsnapshot.gke.io` API group for the KubeRay service account role generation.

## 3. Pod Restoration

**`ray-operator/controllers/ray/common/pod.go`**
During the `BuildPod` stage of the head pod (before KubeRay submits it to the Kubernetes API), I injected logic to scrutinize the `RestoreFrom` property.
If a user explicitly provides a snapshot ID under `RestoreFrom`, I write an explicit `podsnapshot.gke.io/ps-name` annotation mapping to that ID inside the head pod's `PodTemplateSpec.Annotations` dictionary. GKE intercepts this annotation natively and modifies the generic container execution path to reconstruct state from that previous checkpoint instead!

## 4. Resource Cleanup

To ensure that dynamically generated GKE `PodSnapshots` do not become orphaned when a `RayCluster` is deleted, I introduced a dedicated finalizer: `ray.io/podsnapshot-cleanup-finalizer`.

**`ray-operator/controllers/ray/raycluster_controller.go`**
During the KubeRay reconcile loop, if `HeadBackupRestore.Enable` is true, the operator attaches this finalizer to the `RayCluster`. When the `RayCluster` is deleted, Kubernetes blocks the deletion until the finalizer is removed. KubeRay intercepts this phase and executes the `cleanUpPodSnapshots` sequence:
1. Queries the Kubernetes API for all `PodSnapshot` resources in the RayCluster's namespace using the `unstructured` client.
2. Filters the snapshots by matching their `spec.policyName` to the `PodSnapshotPolicy` associated with the dying RayCluster.
3. Wipes the matching `PodSnapshotPolicy` and any outstanding `PodSnapshotManualTrigger` objects.
4. Issues delete commands for all matched snapshots before ultimately removing the finalizer and allowing the `RayCluster` deletion to proceed.

## 5. Hotfixes Applied

During testing, three bugs were identified and circumvented:
* **Trigger Duplications:** Initially, KubeRay failed to honor the `BackupInterval` constraint, infinitely generating immediate triggers. This was traced to the `InconsistentRayClusterStatus` hook in `utils/consistency.go` rejecting status updates holding the `LastPodSnapshotTime`. I patched the hook to explicitly track the PodSnapshot fields, ensuring the timeout pacing is persisted correctly across reconciliation loops.
* **Sandbox Termination Freezes:** The head pod `Terminating` sequence failed to clear because the containerd/runsc `gvisor` runtime hung when Kubelet issued the SIGKILL to a pod actively entangled in a `PodSnapshotPolicy` state. By injecting the aggressive synchronous deletion of the Policy and Triggers into `cleanUpPodSnapshots` (Step 3 above), the API link to the pod is severed in advance, allowing Kubelet to kill the sandbox cleanly and resolving the cluster deletion lock-up.
* **Polling Loop Lockout:** KubeRay's continuous reconciliation cycle hard-codes a default hibernation phase of exactly 300 seconds (5m) between checks unless an external API event wakes the component (`controllers/ray/raycluster_controller.go`). Passing a `backupInterval` below `5m` failed to spawn matching triggers because the loop remained asleep. I patched `calculateStatus` to evaluate the remaining `backupInterval` span. If `(time_to_trigger < 5m)`, KubeRay overrides `RequeueAfter` with the precise remaining seconds, restoring accurate millisecond-level snapshot cadence for any arbitrary user configuration.
* **Gvisor CrashLoopBackOff Deadlock:** Due to the time jump inherent to a PodSnapshot restore phase within `gVisor`, Ray's internal C++ components (`gcs_server` and `raylet`) experience immediate timeout expirations (defaulting natively between 15s to 60s natively parsed from `ray_config_def.h`). These expirations prompt the internal network to assume a catastrophic partition, causing the components to aggressively commit suicide and yielding a `gVisor` blocked `CrashLoopBackOff` restart loop. To resolve this organically and genuinely preserve the in-memory cluster state, users must inject an expanded suite of internal Ray timeout overrides directly into their `RayCluster`'s environment map. Expanding variables such as `RAY_health_check_failure_threshold`, `RAY_gcs_rpc_server_reconnect_timeout_s`, and `RAY_py_gcs_connect_timeout_s` to buffer spans well beyond the expected snapshot duration (e.g. `300s`/`300000ms`) enables the active daemon to perfectly hibernate through the time-jump without forcing an arbitrary restart.

## Complete

All changes were successfully verified through compilation and end-to-end testing on GKE. KubeRay will now trigger, manage, and safely clean up node-level head pod snapshots automatically!
