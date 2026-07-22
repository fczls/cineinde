# Mapping couleur — rationalisation

> **✅ APPLIQUÉ le 2026-07-22.** Couleurs brutes distinctes : **58 → 11** (toutes ×1).
> Décisions : mini-ramp neutre (`--neutral-lo/mid/hi`) · `#55473A → --gold-dim`.
> ⚠️ Changements *délibérés* (snapping ≤ .05 alpha / quelques L) — **à valider à l'œil**.
>
> **One-offs restants (11, assumés — futur allowlist du lint P3)** : dégradés & teintes rares,
> non tokenisables sans `color-mix` : `rgba(12,11,11,0/.97)` (fond nav), `rgba(17,16,16,0/.32/.4)`,
> `rgba(120,105,91,0/.30)` (brun en dégradé), `rgba(135,135,135,.77)`, `rgba(102,102,102,.20)`,
> `rgba(207,207,207,1)`, `#1c1608`.

---

## (Proposition d'origine, pour trace)

> Cible : ramener les 55 couleurs brutes de `src/components.css` sur des tokens.
> Sémantique **t-shirt** (sm/md/lg/xl) pour tous les overlays, blancs comme noirs.
> Delta = écart de valeur (imperceptible visé). ✅ snap propre · 🆕 nouveau token · ⚠️ décision.

## Nouveaux tokens proposés

### Overlays clairs (blanc sur fond sombre — fills, séparateurs)
Les bordures existantes `--border` (.07) et `--border2` (.13) sont **conservées** (concept sémantique « bordure », très utilisé). Les blancs proches y sont repliés ; seuls les fills faibles/forts deviennent des tokens.

| Nouveau token | Valeur |
|---|---|
| `--overlay-sm` | `rgba(255,255,255,.04)` |
| `--overlay-lg` | `rgba(255,255,255,.28)` |

### Overlays sombres (scrims, ombres) — échelle t-shirt
| Nouveau token | Valeur |
|---|---|
| `--scrim-sm` | `rgba(0,0,0,.25)` |
| `--scrim-md` | `rgba(0,0,0,.40)` |
| `--scrim-lg` | `rgba(0,0,0,.55)` |
| `--scrim-xl` | `rgba(0,0,0,.72)` |
| `--scrim-2xl` | `rgba(0,0,0,.85)` |

### Overlays or (dérivés de `--gold`)
| Nouveau token | Valeur |
|---|---|
| `--overlay-gold-sm` | `rgba(201,168,76,.15)` |
| `--overlay-gold-lg` | `rgba(201,168,76,.50)` |

---

## Mapping — overlays clairs

| Brut | ×  | Cible | Delta |
|---|---|---|---|
| `rgba(255,255,255,.015)` | 1 | `--overlay-sm` (.04) | +.025 ✅ |
| `rgba(255,255,255,.025)` | 1 | `--overlay-sm` | +.015 ✅ |
| `rgba(255,255,255,.03)` | 3 | `--overlay-sm` | +.01 ✅ |
| `rgba(255,255,255,.04)` (+`0.04`) | 4 | `--overlay-sm` | 0 ✅ |
| `rgba(255,255,255,.05)` | 1 | `--overlay-sm` | −.01 ✅ |
| `rgba(255,255,255,.06)` | 2 | `--border` (.07) | +.01 ✅ |
| `rgba(255,255,255,.08)` | 3 | `--border` (.07) | −.01 ✅ |
| `rgba(255,255,255,.14)` | 1 | `--border2` (.13) | −.01 ✅ |
| `rgba(255,255,255,.28)` | 1 | `--overlay-lg` | 0 ✅ |
| `rgba(255,255,255,.3)` | 3 | `--overlay-lg` (.28) | −.02 ✅ |

## Mapping — overlays sombres

| Brut | ×  | Cible | Delta |
|---|---|---|---|
| `rgba(0,0,0,0.20)` | 2 | `--scrim-sm` (.25) | +.05 ✅ |
| `rgba(0,0,0,.25)` | 1 | `--scrim-sm` | 0 ✅ |
| `rgba(0,0,0,.38)` | 1 | `--scrim-md` (.40) | +.02 ✅ |
| `rgba(0,0,0,.4)` | 1 | `--scrim-md` | 0 ✅ |
| `rgba(0,0,0,.5)` | 2 | `--scrim-lg` (.55) | +.05 ✅ |
| `rgba(0,0,0,.55)` | 2 | `--scrim-lg` | 0 ✅ |
| `rgba(0,0,0,.6)` | 2 | `--scrim-lg` (.55) | −.05 ✅ |
| `rgba(0,0,0,.7)` | 3 | `--scrim-xl` (.72) | +.02 ✅ |
| `rgba(0,0,0,.72)` | 2 | `--scrim-xl` | 0 ✅ |
| `rgba(0,0,0,.75)` | 1 | `--scrim-xl` (.72) | −.03 ✅ |
| `rgba(0,0,0,.85)` | 1 | `--scrim-2xl` | 0 ✅ |

## Mapping — or à alpha

| Brut | ×  | Cible | Delta |
|---|---|---|---|
| `rgba(201,168,76,.12)` | 2 | `--overlay-gold-sm` (.15) | +.03 ✅ |
| `rgba(201,168,76,.2)` (+`0.20`) | 2 | `--overlay-gold-sm` (.15) | −.05 ✅ |
| `rgba(201,168,76,.5)` | 1 | `--overlay-gold-lg` (.50) | 0 ✅ |

## Mapping — solides sombres (→ échelle `--bg*` existante)

| Brut | L | ×  | Cible | Note |
|---|---|---|---|---|
| `#0b0b0b` / `#0c0b0b` | 11 | 4 | `--bg` (#000) | −L11, très sombre ✅ |
| `rgba(12,11,11,.97)` | ~11 | 1 | `--bg` | ✅ |
| `#111` | 17 | 5 | `--bg2` (#191817) | +L8 ✅ |
| `#151413` / `#151515` | 20-21 | 2 | `--bg2` | ✅ |
| `#1a1a1a` / `#1F1919` | 26 | 2 | `--bg2` | ✅ |
| `rgba(31,31,31,1)` (#1f1f1f) | 31 | 1 | `--bg3` (#201f1d) | ✅ |
| `rgba(41,40,39,1)` (#292827) | ~39 | 1 | `--bg4` (#272523) | ✅ |
| `#33302E` | 48 | 1 | `--bg4` | +L10 ✅ |
| `#1c1608` | 22 | 1 | `--bg2` | ⚠️ teinte or perdue (mineur) |

## Mapping — or clair & warm (→ tokens chauds existants)

| Brut | ×  | Cible | Note |
|---|---|---|---|
| `#D0A636` | 2 | `--gold` (#c9a84c) | L167→172 ✅ |
| `rgba(168,160,148,1)` (#a8a094) | 1 | `--text-mid` (#a09080) | ✅ quasi identique |
| `#78695B` | 1 | `--text-dim` (#7a7060) | ⚠️ brun, delta faible |
| `#55473A` | 3 | `--gold-dim` (#7a6430) ? | ⚠️ brun sombre — **à trancher** |
| `rgba(120,105,91,.30)` | 1 | `--overlay` brun ? | ⚠️ voir ci-dessous |

## ⚠️ Le vrai point de décision : les gris NEUTRES

La palette est **chaude** (bruns/ors). Or ces valeurs sont des gris **neutres** — les snapper vers un token chaud décale la teinte (perceptible) :

| Brut | L | ×  | Options |
|---|---|---|---|
| `#3a3a3a` | 58 | 3 | neutre |
| `#4a4a4a` | 74 | 1 | neutre |
| `#666` | 102 | 1 | neutre |
| `rgba(135,135,135,.77)` | ~135 | 1 | neutre |
| `#848484` | 132 | 2 | neutre |
| `rgba(207,207,207,1)` (#cfcfcf) | 207 | 1 | neutre clair |

**Trois options :**
1. **Ajouter un mini-ramp neutre** : `--neutral-lo` (#3a3a3a), `--neutral-mid` (#666), `--neutral-hi` (#848484) — assume que le DS a une famille neutre à côté de la chaude. *Recommandé* : honnête, pas de décalage de teinte.
2. **Snapper vers les tokens chauds** (`#848484`→`--text-mid`…) — palette plus petite, mais **décalage de teinte perceptible**.
3. **Garder en one-offs documentés** — pas de token, mais reste hors système.
