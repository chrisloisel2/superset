from flask import Blueprint, jsonify
import os
import time

from kubernetes import client, config

admin_bp = Blueprint("admin", __name__)

_LOG_TAIL_CHARS = 4096
_REGISTER_SCRIPT_CM = "register-sessions-script"

def _tail(text: str) -> str:
    if not text:
        return ""
    return text[-_LOG_TAIL_CHARS:]


def _get_batch_client():
    try:
        config.load_incluster_config()
    except Exception:
        config.load_kube_config()
    return client.BatchV1Api()


def _job_spec(namespace: str) -> client.V1Job:
    image = os.environ.get("REGISTER_JOB_IMAGE", "touil/hive-tez:latest")
    name = f"register-sessions-{int(time.time())}"

    volume = client.V1Volume(
        name="register-script",
        config_map=client.V1ConfigMapVolumeSource(name=_REGISTER_SCRIPT_CM, default_mode=0o755),
    )
    mount = client.V1VolumeMount(name="register-script", mount_path="/scripts")
    container = client.V1Container(
        name="register",
        image=image,
        command=["python3", "/scripts/register_sessions.py"],
        volume_mounts=[mount],
    )
    template = client.V1PodTemplateSpec(
        metadata=client.V1ObjectMeta(labels={"app": "register-sessions"}),
        spec=client.V1PodSpec(restart_policy="Never", containers=[container], volumes=[volume]),
    )
    spec = client.V1JobSpec(template=template, backoff_limit=2, ttl_seconds_after_finished=600)
    return client.V1Job(metadata=client.V1ObjectMeta(name=name, namespace=namespace), spec=spec)


@admin_bp.route("/api/admin/register-sessions", methods=["POST"])
def register_sessions():
    namespace = os.environ.get("K8S_NAMESPACE", "red-hadoop")

    try:
        batch = _get_batch_client()
        job = _job_spec(namespace)
        created = batch.create_namespaced_job(namespace=namespace, body=job)
        return jsonify(
            {
                "status": "ok",
                "message": "register_sessions job created",
                "namespace": namespace,
                "job_name": created.metadata.name,
            }
        )
    except Exception as e:
        return (
            jsonify(
                {
                    "status": "error",
                    "message": f"failed to create register_sessions job: {e}",
                    "stdout_tail": "",
                    "stderr_tail": _tail(str(e)),
                }
            ),
            500,
        )
