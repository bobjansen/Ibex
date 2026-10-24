"""Fail closed on incomplete, unchecked or mixed-run H2O artifacts."""
import csv
import hashlib
import io
import json
import math
import re
import tarfile

from .answers import CHECK_VERSION, ENGINES, RUN_CONFIG, compare, contract


def validate(text, manifest, publication=True):
    if manifest.get("version") != CHECK_VERSION or manifest.get("status") != "passed":
        raise ValueError("H2O run lacks passed answer validation")
    if hashlib.sha256(text.encode()).hexdigest() != manifest.get("tsv_sha256"):
        raise ValueError("H2O result TSV does not match its checks sidecar")
    engines = manifest.get("engines", [])
    if (type(manifest.get("threads")) is not int or manifest["threads"] < 1
            or not isinstance(manifest.get("run_id"), str) or not manifest["run_id"]):
        raise ValueError("invalid H2O run identity or core count")
    if len(engines) != len(set(engines)) or len(engines) < 2 or not set(engines) <= set(ENGINES):
        raise ValueError("invalid H2O engine coverage")
    if publication and set(engines) != set(ENGINES):
        raise ValueError("H2O publication requires all three engines")
    if manifest.get("config") != RUN_CONFIG:
        raise ValueError("H2O run has missing or incompatible engine configuration")
    if publication and manifest.get("input_null_token") != "":
        raise ValueError("H2O publication requires the upstream empty-field null encoding")
    valid_sha = lambda value: isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value)
    if not manifest.get("harness") or not all(valid_sha(h) for h in manifest["harness"].values()):
        raise ValueError("H2O run lacks harness provenance")
    if "ibex" in engines:
        origin = manifest.get("provenance") or {}
        artifacts = origin.get("artifacts", {})
        if (origin.get("build_type") != "Release" or not origin.get("sources")
                or not all(valid_sha(h) for h in origin["sources"].values())
                or not all(valid_sha(artifacts.get(name)) for name in
                           ("tools/ibex", "tools/csv.so", "tools/csv.ibex", "CMakeCache.txt"))
                or manifest.get("versions", {}).get("ibex") != "sha256:" + artifacts["tools/ibex"]):
            raise ValueError("H2O run lacks exact Release executable provenance")
    preflight = manifest.get("preflight", {})
    if preflight.get("status") != "passed":
        raise ValueError("H2O bounded full answers did not pass")
    expected_small = set()
    for data, task in (("G1_768_2_0_0", "groupby"), ("G1_768_2_5_1", "groupby"),
                       ("J1_1e7_NA_0_0", "join"), ("J1_1e7_NA_5_1", "join")):
        for cores in {1, min(2, manifest["threads"])}:
            for i in range(1, 11 if task == "groupby" else 6):
                expected_small.add((data, task, f"q{i}", cores))
    actual_small = []
    for check in preflight.get("checks", []):
        if check["status"] != "ok" or check["engines"] != engines:
            raise ValueError("H2O bounded answer check failed or lacks engines")
        actual_small.append((check["data"], check["task"], check["question"], check["cores"]))
    if len(actual_small) != len(set(actual_small)) or set(actual_small) != expected_small:
        raise ValueError("incomplete H2O bounded answer coverage")
    expected, answers = set(), {}
    data_names = manifest.get("data", [])
    if not data_names or len(set(data_names)) != len(data_names):
        raise ValueError("invalid H2O dataset coverage")
    if set(manifest.get("questions", {})) != set(data_names):
        raise ValueError("missing H2O question plan")
    for data in data_names:
        task = "groupby" if data.startswith("G1_") else "join" if data.startswith("J1_") else None
        all_questions = {f"q{i}" for i in range(1, 11 if task == "groupby" else 6)}
        questions = manifest["questions"][data]
        if (task is None or not questions or len(questions) != len(set(questions))
                or not set(questions) <= all_questions or publication and set(questions) != all_questions):
            raise ValueError("H2O publication requires every question")
        expected.update((data, q, engine) for q in questions for engine in engines)
    for result in manifest.get("results", []):
        data, task = result["data"], result["task"]
        if task != ("groupby" if data.startswith("G1_") else "join"):
            raise ValueError("incorrect H2O task")
        for row in result["rows"]:
            q, engine = row["question"], row["engine"]
            key = data, q, engine
            if key not in expected or key in answers or row["error"]:
                raise ValueError("failed, duplicate or unexpected H2O answer")
            answer = row.get("answer", {})
            if answer.get("schema") != contract(task, q) or answer.get("rows") != row["out_rows"]:
                raise ValueError("missing or invalid H2O answer contract")
            samples = row["times"]
            if (not samples or any(not math.isfinite(t) or t <= 0 for t in samples)
                    or row["min_ms"] != min(samples)):
                raise ValueError("invalid H2O timing samples")
            answers[key] = row
    if set(answers) != expected:
        raise ValueError("missing H2O answer results")
    for data, q, engine in expected:
        error = compare(answers[data, q, engines[0]]["answer"], answers[data, q, engine]["answer"])
        if error:
            raise ValueError(f"H2O {data} {q}: {error}")
    rows = list(csv.DictReader(io.StringIO(text, newline=""), delimiter="\t"))
    seen = set()
    for row in rows:
        key = row["data"], row["question"], row["engine"]
        if key not in expected or key in seen:
            raise ValueError("missing, duplicate or unexpected H2O timing cell")
        seen.add(key)
        source = answers[key]
        if (row["status"] != "ok" or row.get("validation") != CHECK_VERSION
                or row.get("run_id") != manifest.get("run_id")
                or row.get("version") != manifest.get("versions", {}).get(row["engine"])
                or int(row["threads"]) != manifest["threads"]
                or int(row["out_rows"]) != source["out_rows"]
                or int(row["iters"]) != len(source["times"])
                or not math.isfinite(float(row["min_ms"])) or float(row["min_ms"]) <= 0
                or row["min_ms"] != f"{source['min_ms']:.3f}"):
            raise ValueError("unchecked or inconsistent H2O timing cell")
    if seen != expected:
        raise ValueError("incomplete H2O timing coverage")
    return rows


def read(text, sidecar, publication=True):
    try:
        return validate(text, json.loads(sidecar), publication)
    except (KeyError, TypeError, OverflowError, json.JSONDecodeError) as exc:
        raise ValueError(f"malformed H2O checks: {exc}") from exc


def validate_plan(plan, files):
    data, cores = plan["data"], plan["cores"]
    if (not data or len(data) != len(set(data)) or not cores or len(cores) != len(set(cores))
            or any(type(c) is not int or c < 1 for c in cores)):
        raise ValueError("invalid H2O run plan")
    expected = {f"h2o_{c}c.tsv" for c in cores}
    if {name for name in files if re.fullmatch(r"h2o_[^/]*\.tsv", name)} != expected:
        raise ValueError("incomplete or unexpected H2O core counts")
    if files.get("failures.txt", "").strip():
        raise ValueError("H2O worker failures")
    for c in cores:
        path = f"h2o_{c}c.tsv"
        sidecar = files[path + ".checks.json"]
        manifest = json.loads(sidecar)
        if manifest.get("data") != data or manifest.get("threads") != c:
            raise ValueError("H2O worker does not match requested run plan")
        read(files[path], sidecar)


def archive_inputs(path):
    """Read only root result files; do not extract paths supplied by an archive."""
    files = {}
    total = 0
    with tarfile.open(path, "r:gz") as archive:
        for member in archive:
            if not re.fullmatch(r"results/[^/]+", member.name):
                continue
            name = member.name[len("results/"):]
            if name not in {"run.json", "exit_code.txt", "failures.txt"} and not re.fullmatch(
                    r"h2o_[^/]*\.tsv(?:\.checks\.json)?", name):
                continue
            total += member.size
            if not member.isfile() or name in files or member.size > 128 * 1024**2 or total > 512 * 1024**2:
                raise ValueError("invalid or oversized H2O archive member")
            files[name] = archive.extractfile(member).read().decode("utf-8")
    if files.get("exit_code.txt", "").strip() != "exit_code=0":
        raise ValueError("failed or incomplete H2O archive")
    try:
        validate_plan(json.loads(files["run.json"]), files)
    except (KeyError, TypeError) as exc:
        raise ValueError(f"incomplete H2O archive: {exc}") from exc
    return [(f"{path}:results/{name}", text, files[name + ".checks.json"])
            for name, text in files.items() if re.fullmatch(r"h2o_[^/]*\.tsv", name)]
