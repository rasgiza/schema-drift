"""Create the schema-drift demo workspace and publish the lakehouse, notebooks, and pipeline.

Uses the signed-in Azure CLI identity against the Fabric REST API.

Run it against your own Fabric capacity:

    az login
    python deploy_fabric.py --capacity-id <your-fabric-capacity-guid>

Find the capacity id with:

    az rest --method get --url "https://api.fabric.microsoft.com/v1/capacities" --resource https://api.fabric.microsoft.com

Optional flags:
    --workspace-name   Defaults to schema-drift-demo.

Environment variables FABRIC_CAPACITY_ID and FABRIC_WORKSPACE_NAME work too.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import shutil
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent
WORKSPACE_NAME = os.environ.get("FABRIC_WORKSPACE_NAME", "schema-drift-demo")
CAPACITY_ID = os.environ.get("FABRIC_CAPACITY_ID", "")
API = "https://api.fabric.microsoft.com/v1"
DESCRIPTION = (
    "Synthetic medallion schema-drift demo. No PHI. "
    "The Silver notebook implements the contract gate; Fabric does not do that automatically."
)


def az_command() -> list[str]:
    found = shutil.which("az")
    if found:
        return [found]
    for candidate in (
        r"C:\Program Files\Microsoft SDKs\Azure\CLI2\wbin\az.cmd",
        r"C:\Program Files (x86)\Microsoft SDKs\Azure\CLI2\wbin\az.cmd",
    ):
        if Path(candidate).exists():
            return ["cmd", "/c", candidate]
    raise SystemExit("Azure CLI not found. Install it, then run 'az login'.")


def token(resource: str = "https://api.fabric.microsoft.com") -> str:
    try:
        return subprocess.check_output(
            az_command()
            + [
                "account",
                "get-access-token",
                "--resource",
                resource,
                "--query",
                "accessToken",
                "-o",
                "tsv",
            ],
            text=True,
        ).strip()
    except subprocess.CalledProcessError as error:
        raise SystemExit("Could not get a token. Run 'az login' first.") from error


def call(method: str, url: str, body: dict | None = None, tok: str | None = None) -> tuple[int, dict, dict]:
    data = None if body is None else json.dumps(body).encode("utf-8")
    request = urllib.request.Request(url, data=data, method=method)
    request.add_header("Authorization", f"Bearer {tok or token()}")
    request.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(request) as response:
            raw = response.read().decode("utf-8")
            headers = {key.lower(): value for key, value in response.headers.items()}
            return response.status, json.loads(raw) if raw else {}, headers
    except urllib.error.HTTPError as error:
        raw = error.read().decode("utf-8")
        raise SystemExit(f"{method} {url} failed {error.code}: {raw}") from error


def wait_operation(headers: dict, tok: str) -> dict:
    location = headers.get("location")
    if not location:
        return {}
    while True:
        status, payload, op_headers = call("GET", location, tok=tok)
        state = payload.get("status")
        print(f"operation {state}")
        if state in {"Succeeded", "Failed"}:
            if state == "Failed":
                raise SystemExit(json.dumps(payload))
            result_url = op_headers.get("location") or location.rstrip("/") + "/result"
            if not result_url.endswith("/result"):
                result_url = location.rstrip("/") + "/result"
            _, result, _ = call("GET", result_url, tok=tok)
            return result
        time.sleep(int(op_headers.get("retry-after", "5")))


def create_or_get(url: str, body: dict, tok: str) -> dict:
    data = json.dumps(body).encode("utf-8")
    request = urllib.request.Request(url, data=data, method="POST")
    request.add_header("Authorization", f"Bearer {tok}")
    request.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(request) as response:
            raw = response.read().decode("utf-8")
            headers = {key.lower(): value for key, value in response.headers.items()}
            created = json.loads(raw) if raw else {}
            if response.status == 202:
                return wait_operation(headers, tok)
            return created
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8")
        if error.code == 409 or "ItemDisplayNameAlreadyInUse" in detail or "WorkspaceNameAlreadyExists" in detail:
            return {}
        raise SystemExit(f"POST {url} failed {error.code}: {detail}") from error


def part(path: Path) -> dict:
    return {
        "path": path.name,
        "payload": base64.b64encode(path.read_bytes()).decode("ascii"),
        "payloadType": "InlineBase64",
    }


ITEM_PATHS = {
    "Notebook": "notebooks",
    "DataPipeline": "dataPipelines",
    "Lakehouse": "lakehouses",
}


def list_items(workspace_id: str, item_type: str, tok: str) -> list[dict]:
    items: list[dict] = []
    url = f"{API}/workspaces/{workspace_id}/items?type={item_type}"
    while url:
        _, listing, _ = call("GET", url, tok=tok)
        items.extend(listing.get("value", []))
        token_value = listing.get("continuationToken")
        url = f"{API}/workspaces/{workspace_id}/items?type={item_type}&continuationToken={token_value}" if token_value else ""
    return items


def upsert_item(workspace_id: str, item_type: str, display_name: str, definition: dict, tok: str) -> dict:
    listing = {"value": list_items(workspace_id, item_type, tok)}
    existing = next((item for item in listing.get("value", []) if item["displayName"] == display_name), None)
    if existing:
        print(f"updating {display_name}")
        status_request = urllib.request.Request(
            f"{API}/workspaces/{workspace_id}/items/{existing['id']}/updateDefinition",
            data=json.dumps({"definition": definition}).encode("utf-8"),
            method="POST",
        )
        status_request.add_header("Authorization", f"Bearer {tok}")
        status_request.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(status_request) as response:
                headers = {key.lower(): value for key, value in response.headers.items()}
                if response.status == 202:
                    wait_operation(headers, tok)
        except urllib.error.HTTPError as error:
            raise SystemExit(error.read().decode("utf-8")) from error
        return existing
    print(f"creating {display_name}")
    created = create_or_get(
        f"{API}/workspaces/{workspace_id}/{ITEM_PATHS[item_type]}",
        {"displayName": display_name, "description": DESCRIPTION, "definition": definition},
        tok,
    )
    if created:
        return created
    _, listing, _ = call("GET", f"{API}/workspaces/{workspace_id}/items?type={item_type}", tok=tok)
    return next(item for item in listing["value"] if item["displayName"] == display_name)


def render_notebook(folder: str, workspace_id: str, lakehouse_id: str) -> dict:
    source = ROOT / "workspace" / folder / "notebook-content.py"
    text = source.read_text(encoding="utf-8").replace("__WORKSPACE_ID__", workspace_id).replace("__LAKEHOUSE_ID__", lakehouse_id)
    rendered = ROOT / "workspace" / folder / "notebook-content.rendered.py"
    rendered.write_text(text, encoding="utf-8")
    platform = ROOT / "workspace" / folder / ".platform"
    return {
        "format": "fabricGitSource",
        "parts": [
            {
                "path": "notebook-content.py",
                "payload": base64.b64encode(text.encode("utf-8")).decode("ascii"),
                "payloadType": "InlineBase64",
            },
            part(platform),
        ],
    }


def pipeline_definition(workspace_id: str, gate_id: str, gold_id: str) -> dict:
    content = {
        "properties": {
            "description": "Silver schema gate, then Gold release only if the gate activity succeeds.",
            "activities": [
                {
                    "name": "NB_Schema_Gate",
                    "type": "TridentNotebook",
                    "dependsOn": [],
                    "policy": {
                        "timeout": "0.12:00:00",
                        "retry": 0,
                        "retryIntervalInSeconds": 30,
                        "secureOutput": False,
                        "secureInput": False,
                    },
                    "typeProperties": {
                        "notebookId": gate_id,
                        "workspaceId": workspace_id,
                        "parameters": {
                            "batch_name": {
                                "value": {"value": "@pipeline().parameters.batch_name", "type": "Expression"},
                                "type": "string",
                            },
                            "fail_on_quarantine": {
                                "value": {"value": "true", "type": "Expression"},
                                "type": "string",
                            },
                            "write_gold": {
                                "value": {"value": "false", "type": "Expression"},
                                "type": "string",
                            },
                        },
                    },
                },
                {
                    "name": "NB_Gold_Release",
                    "type": "TridentNotebook",
                    "dependsOn": [{"activity": "NB_Schema_Gate", "dependencyConditions": ["Succeeded"]}],
                    "policy": {
                        "timeout": "0.12:00:00",
                        "retry": 0,
                        "retryIntervalInSeconds": 30,
                        "secureOutput": False,
                        "secureInput": False,
                    },
                    "typeProperties": {
                        "notebookId": gold_id,
                        "workspaceId": workspace_id,
                        "parameters": {
                            "batch_name": {
                                "value": {"value": "@pipeline().parameters.batch_name", "type": "Expression"},
                                "type": "string",
                            }
                        },
                    },
                },
            ],
            "parameters": {"batch_name": {"type": "string", "defaultValue": "batch-clean"}},
        }
    }
    path = ROOT / "workspace" / "PL_Schema_Gate.DataPipeline" / "pipeline-content.json"
    path.write_text(json.dumps(content, indent=2), encoding="utf-8")
    return {
        "parts": [
            part(path),
            part(ROOT / "workspace" / "PL_Schema_Gate.DataPipeline" / ".platform"),
        ]
    }


def ensure_directory(workspace_id: str, lakehouse_id: str, remote_dir: str, tok: str) -> None:
    request = urllib.request.Request(
        f"https://onelake.dfs.fabric.microsoft.com/{workspace_id}/{lakehouse_id}/{remote_dir}?resource=directory",
        method="PUT",
        data=b"",
    )
    request.add_header("Authorization", f"Bearer {tok}")
    request.add_header("x-ms-version", "2023-11-03")
    try:
        urllib.request.urlopen(request).close()
    except urllib.error.HTTPError as error:
        if error.code not in {409}:
            raise SystemExit(f"mkdir {remote_dir} failed {error.code}: {error.read().decode('utf-8')}") from error


def upload_file(workspace_id: str, lakehouse_id: str, remote_path: str, local_path: Path, tok: str) -> None:
    base = f"https://onelake.dfs.fabric.microsoft.com/{workspace_id}/{lakehouse_id}"
    create = urllib.request.Request(f"{base}/{remote_path}?resource=file", method="PUT", data=b"")
    create.add_header("Authorization", f"Bearer {tok}")
    create.add_header("x-ms-version", "2023-11-03")
    try:
        urllib.request.urlopen(create).close()
    except urllib.error.HTTPError as error:
        if error.code not in {409}:
            detail = error.read().decode("utf-8")
            raise SystemExit(f"create {remote_path} failed {error.code}: {detail}") from error
    payload = local_path.read_bytes()
    append = urllib.request.Request(
        f"{base}/{remote_path}?action=append&position=0",
        data=payload,
        method="PATCH",
    )
    append.add_header("Authorization", f"Bearer {tok}")
    append.add_header("x-ms-version", "2023-11-03")
    append.add_header("Content-Type", "application/octet-stream")
    try:
        urllib.request.urlopen(append).close()
    except urllib.error.HTTPError as error:
        raise SystemExit(f"append {remote_path} failed {error.code}: {error.read().decode('utf-8')}") from error
    flush = urllib.request.Request(
        f"{base}/{remote_path}?action=flush&position={len(payload)}",
        method="PATCH",
        data=b"",
    )
    flush.add_header("Authorization", f"Bearer {tok}")
    flush.add_header("x-ms-version", "2023-11-03")
    try:
        urllib.request.urlopen(flush).close()
    except urllib.error.HTTPError as error:
        raise SystemExit(f"flush {remote_path} failed {error.code}: {error.read().decode('utf-8')}") from error
    print(f"uploaded {remote_path}")


def main() -> None:
    global WORKSPACE_NAME, CAPACITY_ID
    parser = argparse.ArgumentParser(description="Publish the schema-drift demo to a Fabric workspace.")
    parser.add_argument("--capacity-id", default=CAPACITY_ID, help="Fabric capacity GUID that will host the workspace.")
    parser.add_argument("--workspace-name", default=WORKSPACE_NAME, help="Workspace to create or update.")
    args = parser.parse_args()
    WORKSPACE_NAME = args.workspace_name
    CAPACITY_ID = args.capacity_id

    tok = token()
    _, workspaces, _ = call("GET", f"{API}/workspaces", tok=tok)
    workspace = next((item for item in workspaces.get("value", []) if item["displayName"] == WORKSPACE_NAME), None)
    if workspace is None:
        if not CAPACITY_ID:
            raise SystemExit(
                "Workspace does not exist yet, so a capacity is required.\n"
                "Pass --capacity-id <guid> or set FABRIC_CAPACITY_ID.\n"
                "List capacities: az rest --method get "
                '--url "https://api.fabric.microsoft.com/v1/capacities" '
                "--resource https://api.fabric.microsoft.com"
            )
        print(f"creating workspace {WORKSPACE_NAME}")
        workspace = create_or_get(
            f"{API}/workspaces",
            {"displayName": WORKSPACE_NAME, "description": DESCRIPTION, "capacityId": CAPACITY_ID},
            tok,
        )
        if not workspace:
            _, workspaces, _ = call("GET", f"{API}/workspaces", tok=tok)
            workspace = next(item for item in workspaces["value"] if item["displayName"] == WORKSPACE_NAME)
    workspace_id = workspace["id"]
    print(f"workspace {workspace_id}")

    _, items, _ = call("GET", f"{API}/workspaces/{workspace_id}/items?type=Lakehouse", tok=tok)
    lakehouse = next((item for item in items.get("value", []) if item["displayName"] == "lh_schema_drift"), None)
    if lakehouse is None:
        lakehouse = create_or_get(
            f"{API}/workspaces/{workspace_id}/lakehouses",
            {"displayName": "lh_schema_drift", "description": DESCRIPTION},
            tok,
        )
        if not lakehouse:
            _, items, _ = call("GET", f"{API}/workspaces/{workspace_id}/items?type=Lakehouse", tok=tok)
            lakehouse = next(item for item in items["value"] if item["displayName"] == "lh_schema_drift")
    lakehouse_id = lakehouse["id"]
    print(f"lakehouse {lakehouse_id}")

    gate = upsert_item(
        workspace_id,
        "Notebook",
        "NB_Schema_Gate",
        render_notebook("NB_Schema_Gate.Notebook", workspace_id, lakehouse_id),
        tok,
    )
    gold = upsert_item(
        workspace_id,
        "Notebook",
        "NB_Gold_Release",
        render_notebook("NB_Gold_Release.Notebook", workspace_id, lakehouse_id),
        tok,
    )
    pipeline = upsert_item(
        workspace_id,
        "DataPipeline",
        "PL_Schema_Gate",
        pipeline_definition(workspace_id, gate["id"], gold["id"]),
        tok,
    )

    incoming = ROOT / "data" / "incoming"
    if not any(incoming.glob("*.csv")):
        import sys

        sys.path.insert(0, str(ROOT / "src"))
        from batches import materialize

        materialize(incoming)
    storage_tok = token("https://storage.azure.com")
    ensure_directory(workspace_id, lakehouse_id, "Files/incoming", storage_tok)
    for path in sorted(incoming.iterdir()):
        if path.is_file():
            upload_file(workspace_id, lakehouse_id, f"Files/incoming/{path.name}", path, storage_tok)

    result = {
        "workspaceName": WORKSPACE_NAME,
        "workspaceId": workspace_id,
        "capacityId": CAPACITY_ID,
        "lakehouseId": lakehouse_id,
        "notebooks": {"NB_Schema_Gate": gate["id"], "NB_Gold_Release": gold["id"]},
        "pipelineId": pipeline["id"],
        "portalUrl": f"https://app.fabric.microsoft.com/groups/{workspace_id}/list",
    }
    (ROOT / "deployed.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
