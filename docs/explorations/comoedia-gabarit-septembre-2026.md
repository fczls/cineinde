# Le PDF Comoedia dit plus que ce qu'on en lit

**Statut : exploration.** Ce document constate et propose ; il ne décide pas et
n'engage aucun développement. Chaque chiffre vient d'une mesure sur le PDF de la
semaine du 16 septembre 2026, rejouable par `tools/inspect_pdf_comoedia.py`.

Point de départ : la maquette de septembre 2026 a cassé le parsing (cf. le `fix:`
qui ouvre cette branche). En la réparant, on découvre qu'elle est **plus
structurée que l'ancienne** — et que l'essentiel de cette structure tombe
aujourd'hui dans nos expressions régulières sans laisser de trace.

---

## 1. Ce que la maquette expose

| Signal | Où | Mesuré cette semaine | Ce qu'on en fait |
|---|---|---|---|
| Rubrique du film | colonne de garde, page 2 | `ÉVÈNEMENTS` 4, `LES SORTIES` 5, `EN SALLE` 13 | ignoré |
| Renvoi de note par séance | chiffre collé à l'heure | 12 séances sur 271, 8 films | **effacé** |
| Légende des renvois | bas de page 2 | 5 notes : 2 avant-premières, 2 rencontres, 1 ST-SME | jamais lu |
| Qualificatifs | cellule de version | `INT. -12 ANS`, `AVERTISSEMENT`, `DOCUMENTAIRE`, `ANIMATION`, `DÈS 3 ANS`, `JEUNE PUBLIC` | **tronqué** |
| Fiche éditoriale | page 1, 4 colonnes | réalisateur, casting, pays, année, durée, palmarès, synopsis | jamais lu |
| Bloc événement | page 1, rubrique `ÉVÈNEMENTS` | type + date + heure + film + descriptif, 4 blocs | jamais lu |

La page 1 n'est pas un fouillis : elle a quatre colonnes à gouttières franches
(140, 280, 420 pt) et hiérarchise par la taille de police — 18 pt pour les titres
de film et les intertitres de rubrique, 7–8 pt pour le corps. Un regroupement des
mots par abscisse puis par ordonnée la rend lisible bloc par bloc.

### Les renvois de notes portent deux choses très différentes

```
1. Avant-première - En présence du réalisateur Bruno Dumont.
2. Rencontre - Séance unique dans le cadre du festival Play It Again ! … traduite
   en LSF … Séance gratuite sur réservation.
5. Séances ST-SME - Séances de films français sous-titrés en français pour les
   personnes sourdes et/ou malentendantes.
```

Les notes 1 à 4 qualifient **une séance-événement** — une par film, sur un film
qui n'a souvent que cette séance-là. La note 5 est d'une autre nature : elle
marque **8 séances dispersées** parmi les 271, à l'intérieur de films qui en ont
jusqu'à 31. C'est une information par séance, qu'aucune jointure par titre ne
peut reconstituer : dans « Si tu penses bien », trois séances sur trente et une
sont sous-titrées, et rien d'autre que ce chiffre ne le dit.

---

## 2. Où cette information se perd

Trois lignes, trois pertes — toutes en amont, aucune dans le front :

- [`scraper.py:1177`](../../scraper.py) — `cell_clean = re.sub(r"(\d{1,2}[h:]\d{2})(\d)", r"\1", cell)`
  efface le renvoi pour pouvoir lire l'heure. Il est *identifié* puis jeté : le
  capturer ne coûte rien de plus que de ne pas le jeter.
- [`scraper.py:1163`](../../scraper.py) — `version_token = re.split(…)[0]` ne garde
  que le premier segment de `VF / INT. -12 ANS`. Le reste est de l'information de
  classification, pas du bruit.
- [`scraper.py:302`](../../scraper.py) — `detect_version()` réduit toute chaîne à
  l'une de quatre valeurs. C'est le même étranglement côté Zola, où les fiches
  portent `VF VI`, `VO ST`, `VF ST,OCAP,VI` (`VI` = audiodescription, `ST`/`OCAP`
  = sourds et malentendants) : **11 séances sur 14** dans un échantillon de 8
  fiches. Tout est aplati en `VF`.

L'accessibilité arrive donc déjà par **deux sources** et se perd au même endroit.
Ce n'est pas une idée à créer, c'est une donnée à cesser de jeter.

---

## 3. Pistes

### A — Garder les attributs de séance (accessibilité)

**Gain.** Un public pour qui l'information est décisive, et qu'aucune autre page
de Lyon n'agrège. Deux cinémas sur trois l'exposent déjà.

**Mécanisme.** Capturer le renvoi au lieu de l'effacer, résoudre contre la
légende du bas de page ; côté Zola, conserver les jetons au lieu de les réduire.
Une notion commune — `attributs: ["st_sme"]` / `["audiodescription"]` — sur le
dict séance (contrat C1), et une colonne sur `seances` (migration 006).

**Coût.** Une migration, un champ dans le contrat, un rendu dans le front.

**Risque.** Faible et bien borné : un renvoi non résolu donne une séance sans
attribut, jamais une séance fausse. **Mais** un renvoi à deux chiffres serait
indécidable — `20:3010` peut se lire 20:30 + note 10 ou 20:301 + note 0. On en
est à 5 notes ; il faut traiter le cas dès maintenant (refuser et journaliser
plutôt que deviner), pas le découvrir un mardi soir.

### B — Lire la rubrique du film

**Gain.** `LES SORTIES` contre `EN SALLE`, c'est « nouveauté de la semaine »
donné par le programmateur, pas déduit d'une date de sortie nationale qui ne dit
pas ce qu'un cinéma choisit de mettre en avant. Et `ÉVÈNEMENTS` désigne les
films dont les séances sont des séances-événement, sans jointure.

**Mécanisme.** La rubrique est déjà dans la colonne de garde, lue à l'envers par
pdfplumber (`SEITROS SEL`) parce qu'elle est écrite à la verticale ; elle vaut
jusqu'à la rubrique suivante. Quelques lignes.

**Coût.** Un champ, pas de migration si l'on s'en tient à `programme.json` dans
un premier temps.

**Risque.** Faible. C'est du bonus : absent, on retombe sur le comportement
actuel.

### C — Ancrer les événements sur la séance exacte

**Gain.** Aujourd'hui [`resolve_dates_from_seances`](../../scraper.py) reconstruit
le lien événement → séance par **jointure de titre**, puis attache toutes les
séances du film comprises dans l'enveloppe de dates. Le renvoi de note donne le
lien directement : *cette* séance, à *cette* heure, est l'avant-première.

**À ne pas surestimer.** Cette semaine la jointure ne se trompe pas : les films
concernés n'ont qu'une séance. Le risque est structurel, pas observé — il mord le
jour où un film de festival tourne aussi en séance normale, et alors toutes ses
séances deviennent des créneaux du festival.

**Mécanisme.** Le renvoi devient une clé de rapprochement supplémentaire, en
complément de la jointure par titre, jamais à sa place : le PDF ne couvre qu'un
cinéma et qu'une semaine.

**Coût.** Moyen — touche un pipeline mûr et délicat. À traiter séparément.

### D — La page 1 comme repli éditorial de TMDB

**Gain.** Au dernier run, TMDB a échoué sur 6 titres, dont les deux parties de
« La Bataille de Gaulle ». La page 1 porte pour chacune réalisateur, casting,
pays, année, durée, avertissement. Ce sont précisément les films que TMDB ne peut
pas connaître — séances-événement, programmes composites, titres de festival.

**Mécanisme.** Regroupement des mots en colonnes par abscisse, blocs délimités
par les titres à 18 pt, puis trois motifs stables : `De X avec Y…`,
`Pays - Année - Durée`, mentions de palmarès.

**Coût.** Le plus élevé des cinq, et le plus exposé : c'est du texte justifié sur
quatre colonnes, la reconstruction reste approximative — un test a rendu
« Séance animée par Jamin, Alban » au lieu d'« Alban Jamin ». Acceptable pour des
champs courts et cadrés (durée, année, pays) ; **à proscrire pour le synopsis**,
où une phrase recousue de travers serait publiée telle quelle.

**Risque.** Les gouttières sont mesurées sur **un seul** numéro. À confirmer sur
plusieurs avant de s'y fier.

### E — La rubrique ÉVÈNEMENTS de la page 1

**Gain.** Un flux d'événements complet et déjà typé — `CINÉ-GOÛTER`,
`AVANT-PREMIÈRE`, `FESTIVAL` — avec date, heure, film et descriptif :

```
CINÉ-GOÛTER
MERCREDI 16 SEPTEMBRE À 14H00
Chien bleu et autres belles histoire de l'école des loisirs
Projection suivie d'un atelier créatif …
```

Cela se projette presque terme à terme sur le contrat C5. À noter :
`CINÉ-GOÛTER` n'existe pas dans `_COMOEDIA_TYPE_MAP` ([`scraper.py:2212`](../../scraper.py)).

**À ne pas surestimer.** Le site `/tous-les-evenements` reste la meilleure source :
il est typé à la source, porte les affiches et va **au-delà de la semaine** — 13
événements jusqu'en novembre, là où le PDF n'en montre que 4. Le PDF n'est pas un
remplaçant, c'est un **témoin indépendant** : il confirme, il date à l'heure près,
et il couvre le cas où la page web tombe. Vu comme un remplaçant, il ferait
régresser la couverture d'un facteur trois.

**Une réserve de qualité.** Le PDF écrit « Corlina Picos », la légende du bas de
page « Coralina Picos ». Deux témoins qui divergent : il faudra une règle de
priorité explicite, pas un « le dernier écrit gagne ».

---

## 4. Ce que je recommande

| | Valeur | Risque | Coût | Autonome |
|---|---|---|---|---|
| **A** attributs de séance | élevée | faible | faible | oui |
| **B** rubrique | moyenne | faible | très faible | oui |
| **C** ancrage des événements | moyenne | moyen | moyen | non — dépend de A |
| **D** repli éditorial page 1 | moyenne | élevé | élevé | oui |
| **E** événements page 1 | faible | moyen | moyen | non — dépend de D |

**A + B d'abord, ensemble.** Ils partagent le même parsing, ne touchent que
l'extraction, n'enlèvent rien, et A apporte une information qui n'existe nulle
part ailleurs sur le site. C'est aussi le seul lot qui justifie une migration :
autant la faire une fois.

**C ensuite, isolément** — il touche un pipeline mûr, il mérite sa propre PR et
ses propres tests.

**D et E en dernier, ou jamais.** Leur valeur réelle se mesure d'abord : combien
de films par semaine TMDB manque-t-il *durablement* ? Six sur cinquante-trois
cette semaine, dont plusieurs sont des séances-événement qui n'ont pas vocation à
avoir une fiche film. Si le chiffre tombe à deux ou trois, D ne vaut pas son
risque.

## 5. Une règle à poser avant d'ajouter quoi que ce soit

La maquette a changé trois fois d'un coup sans préavis, et le seul symptôme
était `exit 4`. Chaque champ nouveau est une surface de casse supplémentaire.

**Tout champ tiré du PDF doit être facultatif et ne jamais faire échouer un run.**
Le garde-fou asymétrique doit continuer à ne juger que d'une chose — y a-t-il des
séances Comoedia cette semaine — et surtout pas de la présence d'un renvoi, d'une
rubrique ou d'un synopsis. Sinon on convertit un bonus en panne, et on rend le
pipeline plus fragile en croyant l'enrichir.

Corollaire outillé, déjà dans cette branche : `tools/inspect_pdf_comoedia.py`
montre la structure d'un PDF au lieu de la deviner. Le scraper ne dit que
« 0 film » ; cet outil dit pourquoi.

---

## Annexe — reproduire les mesures

```bash
python3 tools/inspect_pdf_comoedia.py <url-ou-fichier.pdf>
```

Rubriques, renvois de notes, géométrie des colonnes de la page éditoriale. Aucune
écriture, aucun effet de bord. Les chiffres de ce document viennent du PDF de la
semaine du 16 septembre 2026 et du site `lezola.com` consulté le 20 septembre 2026.
