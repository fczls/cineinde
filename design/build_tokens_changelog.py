#!/usr/bin/env python3
"""Dérive l'historique des tokens depuis git et l'écrit dans design/tokens-changelog.json.

Pourquoi un fichier committé plutôt qu'un calcul à la volée : la CI (pages.yml) fait un
checkout superficiel (`fetch-depth: 1`), l'historique git n'y est donc pas disponible —
et `build_gallery.py --check` doit rester déterministe. Le JSON est la donnée, ce script
la (re)construit en local.

Une entrée par commit ayant touché design/tokens.json, du plus récent au plus ancien :
tokens ajoutés, valeurs modifiées, notes d'usage modifiées, tokens supprimés. Le résumé
de l'entrée est le sujet du commit.

Conséquence assumée : l'entrée d'un commit ne peut être générée qu'une fois ce commit
écrit. Après avoir committé un changement de tokens, relancer :

    python3 design/build_tokens_changelog.py && python3 build_ui.py

Usage :
    python3 design/build_tokens_changelog.py           # (ré)écrit le JSON
    python3 design/build_tokens_changelog.py --check    # échoue s'il n'est pas à jour
"""
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TOKENS_PATH = "design/tokens.json"
OUT = ROOT / "design" / "tokens-changelog.json"

# groupe DTCG -> (libellé lisible, préfixe de la var CSS ; None = pas de var générée)
GROUPS = {
    "color":      ("Couleurs", ""),
    "fontFamily": ("Familles", "font-"),
    "fontSize":   ("Tailles de police", "fs-"),
    "radius":     ("Rayons", "radius-"),
    "breakpoint": ("Points de rupture", None),
    "spacing":    ("Espacements", None),
}


def _git(*args: str) -> str:
    return subprocess.run(("git", *args), cwd=ROOT, check=True,
                          capture_output=True, text=True).stdout


def _flatten(doc: dict) -> dict:
    """{'color.gold': {'group','key','var','value','desc'}} — les clés $… sont ignorées."""
    out = {}
    for group, node in doc.items():
        if group.startswith("$") or not isinstance(node, dict):
            continue
        label, prefix = GROUPS.get(group, (group, None))
        for key, tok in node.items():
            if key.startswith("$") or not isinstance(tok, dict):
                continue
            out[f"{group}.{key}"] = {
                "group": label,
                "key": key,
                "var": f"--{prefix}{key}" if prefix is not None else None,
                "value": tok.get("$value"),
                "desc": tok.get("$description", ""),
            }
    return out


def _diff(before: dict, after: dict) -> list:
    changes = []
    for path in after.keys() - before.keys():
        t = after[path]
        changes.append({"kind": "added", "path": path, "group": t["group"],
                        "var": t["var"], "to": t["value"], "desc": t["desc"]})
    for path in before.keys() - after.keys():
        t = before[path]
        changes.append({"kind": "removed", "path": path, "group": t["group"],
                        "var": t["var"], "from": t["value"]})
    for path in before.keys() & after.keys():
        b, a = before[path], after[path]
        if b["value"] != a["value"]:
            changes.append({"kind": "changed", "path": path, "group": a["group"],
                            "var": a["var"], "from": b["value"], "to": a["value"],
                            "desc": a["desc"]})
        elif b["desc"] != a["desc"]:
            changes.append({"kind": "note", "path": path, "group": a["group"],
                            "var": a["var"], "to": a["value"], "desc": a["desc"]})
    order = {"added": 0, "changed": 1, "note": 2, "removed": 3}
    changes.sort(key=lambda c: (order[c["kind"]], c["path"]))
    return changes


def collect() -> dict:
    # Garde-fou : sur un clone superficiel (CI), l'historique se résume au dernier
    # commit — régénérer écraserait le fichier par une seule entrée « tout ajouté ».
    if _git("rev-parse", "--is-shallow-repository").strip() == "true":
        sys.exit("ERREUR : clone superficiel — l'historique des tokens n'est pas "
                 "reconstructible ici. Ce script se lance en local (clone complet) ; "
                 "la CI se contente du JSON committé.")
    log = _git("log", "--reverse", "--format=%H\t%ad\t%s", "--date=short",
               "--", TOKENS_PATH).strip().splitlines()
    entries, before = [], {}
    for line in log:
        sha, date, subject = line.split("\t", 2)
        try:
            doc = json.loads(_git("show", f"{sha}:{TOKENS_PATH}"))
        except (subprocess.CalledProcessError, json.JSONDecodeError):
            continue                       # fichier absent/illisible à ce commit
        after = _flatten(doc)
        changes = _diff(before, after)
        if changes:
            entries.append({"date": date, "commit": sha[:7], "summary": subject,
                            "changes": changes})
        before = after
    entries.reverse()                      # le plus récent en tête, comme le CHANGELOG
    return {"$description": "Historique des design tokens, dérivé de l'historique git de "
                            "design/tokens.json par design/build_tokens_changelog.py. "
                            "Ne pas éditer à la main.",
            "entries": entries}


def _serialize(data: dict) -> str:
    return json.dumps(data, ensure_ascii=False, indent=2) + "\n"


def main() -> None:
    payload = _serialize(collect())
    if "--check" in sys.argv:
        current = OUT.read_text(encoding="utf-8") if OUT.exists() else ""
        if current != payload:
            sys.exit("ERREUR : design/tokens-changelog.json n'est pas à jour avec "
                     "l'historique git. Lancer : python3 design/build_tokens_changelog.py")
        print("OK : design/tokens-changelog.json est à jour.")
        return
    OUT.write_text(payload, encoding="utf-8")
    n = len(json.loads(payload)["entries"])
    print(f"design/tokens-changelog.json régénéré ({n} entrée(s)).")


if __name__ == "__main__":
    main()
