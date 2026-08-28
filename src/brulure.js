// ═══════════════════════════════════════════════════════════════════════════
// brulure.js — combustion d'une affiche, en WebGL2. L'image ne s'en va pas :
// elle est CONSUMÉE.
//
// Module PARTAGÉ entre le labo de réglage (design/lab/brulure.html) et la
// production — même dispositif que `cloth.js`, dont il est le pendant : cloth
// fait APPARAÎTRE (0 = froissé → 1 = posé), brulure fait DISPARAÎTRE
// (0 = intacte → 1 = plus rien).
//
// Ce qui est repris de cloth.js : la plomberie WebGL, le recadrage « cover »,
// le débordement du quad, le masque à coins arrondis, l'éclairage par normales,
// l'irisation en palette cosinus, et TOUTE la machinerie de courbes (importée,
// pas recopiée). Ce qui change tient en une idée.
//
// ── L'idée : un CHAMP D'ORDRE DE COMBUSTION ────────────────────────────────
// `ordre(p)` dit QUAND chaque point de l'affiche brûle : 0 = tout de suite,
// 1 = en dernier. Ce champ est FIXE — c'est un seuil qui monte avec le temps et
// le traverse. Tout en découle :
//
//   d = ordre(p) − seuil
//     d < 0            la matière est partie          → alpha 0
//     0 → cendre       liseré carbonisé               → noir, il REMPLACE l'image
//     → braise         ourlet incandescent            → rouge→jaune, il S'AJOUTE
//     → roussi         l'émulsion jaunit puis brunit  → l'image se teinte
//     au-delà          intacte
//
// Un champ figé et un seuil qui monte, c'est ce qui garantit qu'un point brûlé
// ne « repousse » jamais, et qu'un scrub en arrière est exactement réversible.
//
// La GÉOMÉTRIE suit le même champ : le gradient de `ordre` pointe vers ce qui
// brûlera en dernier, donc à l'opposé du feu — la matière chauffée s'y rétracte,
// et c'est ce recul qui fait que les bords se contractent au lieu de rester
// proprement découpés.
//
// ⚠️ CORS : identique à cloth.js — une image cross-origin sans
// `Access-Control-Allow-Origin` ne peut pas devenir une texture, `charger()`
// rejette, l'appelant retombe sur l'<img>.
//
// ⚠️ `prefers-reduced-motion` : le module ne le consulte PAS, comme cloth.js.
// C'est l'appelant qui décide — il est le seul à savoir par quoi remplacer la
// combustion (cf. `_tissuUtilisable()` dans src/template.html, qui refuse
// d'instancier le shader). Une disparition est d'ailleurs plus impérieuse
// qu'une apparition : sous mouvement réduit, il faut toujours prévoir ce qui
// fait sortir l'affiche, sans quoi elle reste à l'écran.
// ═══════════════════════════════════════════════════════════════════════════

import { resoudreEasing } from './cloth.js';

export { bezier, COURBES, EASINGS, resoudreEasing, GABARITS } from './cloth.js';

// ── Réglages par défaut ────────────────────────────────────────────────────
// Toute valeur exposée au labo vit ici. Un preset exporté par le labo est un
// sous-ensemble de cet objet : `{...BRULURE_DEFAUTS, ...preset}` suffit à rejouer.
export const BRULURE_DEFAUTS = {
  // — Temps —
  duree:          2800,   // ms, de l'affiche intacte au vide
  retard:         0,      // ms avant démarrage (décalage entre cartes)
  // Maître de la brûlure. À 0 rien ne brûle et la carte ne fait que se replier ;
  // c'est aussi le seul moyen d'isoler l'un des deux gestes pour le juger.
  combustion:     1.0,

  // — Où le feu prend —
  foyerX:         0.55,   // −1..1 dans le cadre (0,0 = centre)
  foyerY:         0.35,
  // Arbitrage entre deux amorçages. 0 : le feu part du seul foyer, en tache.
  // 1 : il part de TOUT le pourtour et mange vers le cœur — c'est le geste des
  // références, la pellicule qui se rétracte par ses bords.
  // 0.35 tient les deux : le cadre se contracte pendant qu'une bouche mange en
  // travers. Au-delà de 0.5 le pourtour l'emporte et l'affiche se réduit à un
  // rectangle grignoté, ce qui se lit comme un masque animé, pas comme un feu.
  bordure:        0.35,

  // — Forme du front —
  echelle:        2.0,    // taille des lobes de flamme (petit = grandes langues)
  desordre:       0.45,   // irrégularité du front (0 = auréole géométrique)
  detail:         0.45,   // poids des hautes fréquences : la dentelure fine
  bulles:         0.09,   // bouillonnement cellulaire de l'émulsion
  // Des cellules trop fines produisent un front dont la transition tombe sous
  // le pixel : c'est là que l'escalier apparaît. 9 tient l'échantillonnage.
  echelleBulles:  9.0,    // finesse de ce bouillonnement

  // — Bandes du front, en unités de CARTE ————————————————————————————————
  // 1.0 = la hauteur du cadre. 0.045 sur une affiche de 254 px = 11 px, et le
  // même réglage tient sur un écran entier : c'est tout l'intérêt d'une largeur
  // métrique plutôt que « en unités de champ ». Les trois s'empilent depuis le
  // trou vers l'extérieur.
  braise:         0.020,  // ourlet incandescent, sur le front
  cloque:         0.050,  // l'émulsion cuite qui BLANCHIT — cf. laitage
  roussi:         0.070,  // roussissement chaud, le plus en avant du feu
  lueur:          0.80,   // ce que l'ourlet AJOUTE, en plus de sa couleur propre
  laitage:        0.50,   // force du blanchiment de la cloque
  moucheture:     0.30,   // piqûres d'argent semées près du front

  // — La matière brûlée ————————————————————————————————————————————————————
  // Derrière le front, ce n'est PAS un trou : c'est un dépôt de résine figée,
  // boursouflé et vitrifié. Le trou ne vient qu'après, quand ce charbon a fini
  // de se consumer. `magma` est l'épaisseur de ce dépôt, en unités de carte —
  // c'est le réglage qui décide si la brûlure est un liseré ou un continent.
  magma:          0.075,
  echelleBulle:   26.0,   // taille des bulles (grand = fines)
  satellites:     0.55,   // poids des micro-bulles qui cernent les grosses
  // Déformation des bulles, croissante avec la dose : une bulle ne reste pas
  // ronde, elle coule et se presse contre ses voisines. C'est ce qui fait que
  // leur forme ÉVOLUE au lieu de rester figée.
  distorsion:     0.55,
  // Facilité de rupture des parois amincies. C'est lui qui décide si la
  // matière finit en dentelle percée ou en croûte pleine.
  perforation:    0.65,
  boursouflure:   0.55,   // relief des bulles, donc force de leur brillance
  craquelures:    0.60,   // réseau de fissures dans le charbon profond
  vernis:         0.85,   // brillance de la résine figée
  // Grain de la matière brûlée. Il module la DOSE en amont, donc la couleur, la
  // taille des bulles et le percement en héritent — et il hache le spéculaire.
  // C'est le principal antidote à l'« effet 3D » : un dégradé analytique
  // parfaitement lisse est la signature d'une surface calculée.
  granulation:    0.70,
  grainEchelle:   34.0,   // finesse du grain (grand = fin)
  gonflement:     0.055,  // soulèvement d'ensemble (intumescence) — géométrie

  // — Couleurs du feu —
  // Relevées sur les références, et volontairement DÉSATURÉES. Une braise en
  // rouge pur et un halo en jaune d'école, posés à haute fréquence sur une
  // affiche déjà saturée, ne donnent pas du feu : ils donnent une palette
  // 8 bits. Le vrai ourlet est une rouille, et la cendre un bleu de nuit —
  // l'émulsion cuite n'est jamais d'un noir neutre.
  //
  // Les quatre forment une RAMPE, parcourue dans cet ordre par la dose de
  // combustion : halo → caramel → braise → cendre.
  coulHalo:       '#e8cf9a',   // le premier jaunissement
  coulCaramel:    '#c98a3c',   // l'ambre — la teinte dominante des références
  coulBraise:     '#8c3d18',   // le brun rouge
  // Le charbon : un brun très sombre, pas un noir. Poussé au noir il se confond
  // avec un fond sombre et toute la matière brûlée se lit comme un trou — on
  // perd d'un coup les bulles, les craquelures et le vernis.
  coulCendre:     '#241b18',
  coulCloque:     '#d9e0e8',   // le laiteux bleuté de la gélatine décolorée

  // — L'image meurt avant de disparaître —
  delavage:       0.30,   // la chaleur mange les couleurs avant la matière

  // — Matière chauffée (géométrie) —
  // Recul de la matière chauffée, en FRACTION de la zone chaude : c'est
  // directement la compression que subit la matière au front (cf. le shader).
  retraitBord:    0.40,
  retraitGlobal:  0.05,   // contraction d'ensemble de la carte
  gondole:        0.09,   // le film gondole près du front
  largeurChaleur: 0.14,   // étendue de la zone qui travaille, en unités de carte
  nbPlis:         3.5,    // fréquence des vagues PARALLÈLES au front
  echelleGondole: 5.5,    // fréquence du gondolement organique
  perspective:    0.35,   // fuite : le film qui se relève rétrécit

  // — Lumière —
  lumiereX:       -0.45,
  lumiereY:       0.70,
  ombre:          0.45,   // modelé sur le film gondolé
  // Bas : une émulsion cuite est MATE. Un spéculaire marqué sur un film gondolé
  // donne du cellophane sous vide, pas de la pellicule brûlée.
  eclat:          0.12,   // reflet sur les crêtes

  // — Irisation de l'émulsion cuite —
  // Très bas par défaut : la palette cosinus parcourt TOUTES les teintes, verts
  // et magentas compris. Poussée, elle sème des pixels de couleurs qui ne sont
  // dans aucune des références et fait basculer le rendu dans le jeu vidéo.
  iriIntensite:   0.06,
  iriFresnel:     2.20,
  iriEchelle:     1.60,
  iriDecalage:    0.10,

  // — Matière —
  // Le grain n'est pas un défaut à minimiser : c'est lui qui casse le lissé
  // numérique des dégradés et fait lire une surface photographique.
  grain:          0.045,
  opacite:        1.0,

  // — Repli ————————————————————————————————————————————————————————————————
  // Le geste : on saisit le bord BAS de l'affiche et on le rabat sur le bord
  // haut. Modélisé comme une pliure de RAYON fini et non comme une charnière —
  // le pli roule sur un arc de cercle, puis la partie libre repart droite,
  // tangente à cet arc. C'est ce qui rend le mouvement ample plutôt que
  // cassant, et c'est DÉVELOPPABLE : chaque point garde son abscisse
  // curviligne, la feuille ne s'étire nulle part. Comme du papier.
  repli:          0.90,   // 0 = à plat, 1 = rabattu à 180°
  repliDebut:     0.30,   // progression à laquelle le geste s'amorce
  repliCharniere: 0.0,    // position de la pliure (0 = à mi-hauteur)
  repliRayon:     0.09,   // rayon de la pliure — c'est LUI qui fait l'ampleur
  // Basculement de la scène, en degrés. Sans lui, un pli vu de face se projette
  // sur une ligne : juste, et illisible. C'est ce qui donne au rabat une
  // surface visible et au geste son amplitude perçue.
  repliInclinaison: 26.0,

  // — Rendu —
  // Plus fin que cloth : le maillage doit suivre un RETRAIT LOCAL qui varie sur
  // quelques pourcents de la carte. À 64 le bord recule en escalier.
  //
  // Inutile de monter plus haut. La tessellation s'est un temps vue dans les
  // perforations, mais la cause n'était pas la finesse du maillage : c'était
  // `fwidth` (cf. vPente). Corrigée à la source, 64, 96 et 160 deviennent
  // indiscernables même sur un canvas de 1400 px — et 160 coûte 2,5 fois 96.
  subdivisions:   96,
  rayon:          0.094,  // coins arrondis, en fraction du petit côté
  debordement:    1.20,   // marge autour de l'affiche
  ratioCadre:     170 / 254,
};

// Paramètres qui ne vont PAS au shader (pilotage JS ou géométrie du canvas).
const HORS_SHADER = new Set(['duree', 'retard', 'subdivisions', 'debordement']);
// Paramètres passés en `vec3`, saisis en hexadécimal côté labo.
const COULEURS    = new Set(['coulCendre', 'coulBraise', 'coulHalo', 'coulCloque',
                             'coulCaramel']);

/** `#rrggbb` → [r,g,b] dans [0,1]. Pas de conversion d'espace : la texture est
 *  échantillonnée telle quelle, tout le shader travaille en sRGB. */
export function hexVersRvb(hex) {
  const n = parseInt(String(hex).replace('#', ''), 16) || 0;
  return [((n >> 16) & 255) / 255, ((n >> 8) & 255) / 255, (n & 255) / 255];
}

// ── Shaders ────────────────────────────────────────────────────────────────

// Bruits — injectés dans les DEUX étages : le sommet a besoin du champ pour
// déformer, le fragment pour peindre, et les deux doivent lire EXACTEMENT le
// même champ, sinon le trou ne coïnciderait plus avec le retrait.
const BRUITS = `
float hash21(vec2 p) {
  p = fract(p * vec2(123.34, 456.21));
  p += dot(p, p + 45.32);
  return fract(p.x * p.y);
}
vec2 hash22(vec2 p) {
  float n = hash21(p);
  return vec2(n, hash21(p + n * 17.13));
}
// Bruit de valeur, interpolation quintique (dérivée continue — sinon les
// différences finies du sommet feraient facetter les normales).
float bruit(vec2 p) {
  vec2 i = floor(p), f = fract(p);
  vec2 u = f * f * f * (f * (f * 6.0 - 15.0) + 10.0);
  float a = hash21(i);
  float b = hash21(i + vec2(1.0, 0.0));
  float c = hash21(i + vec2(0.0, 1.0));
  float d = hash21(i + vec2(1.0, 1.0));
  return mix(mix(a, b, u.x), mix(c, d, u.x), u.y);
}
// Trois octaves. \`detail\` pondère les deux dernières : à 0 le front est une
// grande langue lisse, à 1 il est dentelé jusqu'au millimètre.
float fbm(vec2 p, float detail) {
  float s = bruit(p), poids = 1.0, a = 1.0;
  for (int i = 1; i < 3; i++) {
    a *= 0.5 * mix(0.30, 1.0, detail);
    p *= 2.03;
    s += a * bruit(p);
    poids += a;
  }
  return s / poids;
}
// Minimum DOUX. Sert à deux endroits, pour la même raison : un min brut laisse
// un pli de gradient là où les deux termes se croisent.
float minDoux(float a, float b, float k) {
  float h = clamp(0.5 + 0.5 * (b - a) / k, 0.0, 1.0);
  return mix(b, a, h) - k * h * (1.0 - h);
}

// Worley F1 — distance au germe le plus proche. C'est LE bruit du
// bouillonnement : il donne des cellules rondes, pas des taches informes.
//
// Assemblé au minimum DOUX, et c'est indispensable : le F1 classique porte une
// arête sur chaque frontière de cellule, et ce pli de gradient se lit comme un
// réseau de POLYGONES dès qu'on éclaire la surface ou qu'on s'en sert pour
// moduler une couleur claire. Même coût, plus de facettes.
float cellules(vec2 p) {
  vec2 i = floor(p), f = fract(p);
  float d = 8.0;
  for (int y = -1; y <= 1; y++) {
    for (int x = -1; x <= 1; x++) {
      vec2 g = vec2(float(x), float(y));
      d = minDoux(d, length(g + hash22(i + g) - f), 0.12);
    }
  }
  return clamp(d, 0.0, 1.0);
}

// Worley enrichi, pour les BULLES de la matière brûlée : renvoie F1, F2 et le
// GRADIENT de F1, en une seule passe.
//
// Le gradient est analytique et gratuit : F1 est une distance à un germe, donc
// son gradient est le vecteur unitaire qui s'en éloigne. C'est ce qui rend les
// bosses de bulles abordables — sans lui il faudrait quatre évaluations de
// Worley en différences finies par pixel, rien que pour l'éclairage.
//
// F2 − F1 sert aux craquelures : cette différence s'annule exactement sur les
// frontières de cellules, c'est-à-dire sur les parois minces entre les vides —
// là où un charbon se fend réellement.
//
// Minimum BRUT ici, contrairement à \`cellules\` : le dôme de la bulle retombe à
// zéro avant la frontière de cellule, donc le pli du min ne s'y voit jamais.
// \`alea\` ressort un tirage propre à la cellule LA PLUS PROCHE — c'est-à-dire à
// la bulle elle-même. Sans lui, toutes les bulles d'une même zone ont
// exactement le même rayon : on obtient du papier bulle, pas de la mousse.
vec4 worley(vec2 p, out float alea) {
  vec2 i = floor(p), f = fract(p);
  float f1 = 8.0, f2 = 8.0;
  vec2 dir = vec2(0.0, 1.0), best = vec2(0.0);
  for (int y = -1; y <= 1; y++) {
    for (int x = -1; x <= 1; x++) {
      vec2 g = vec2(float(x), float(y));
      vec2 v = g + hash22(i + g) - f;
      float d = length(v);
      if (d < f1) { f2 = f1; f1 = d; dir = v; best = g; }
      else if (d < f2) { f2 = d; }
    }
  }
  alea = hash21(i + best + 7.7);
  return vec4(f1, f2, -normalize(dir + vec2(1e-6)));
}

// Bosse d'une bulle, et son gradient, à partir d'un Worley enrichi.
// Profil (1 − u²)² : pente NULLE au bord du dôme. Une calotte sphérique aurait
// une pente infinie sur son cercle et poserait un liseré spéculaire dur tout
// autour de chaque bulle — l'inverse d'une goutte de résine.
//   \`w\`      : sortie de worley()
//   \`r\`      : rayon du dôme, en fraction de cellule
//   \`echelle\`: facteur d'échelle appliqué à p avant worley (chaîne de dérivée)
vec3 bosse(vec4 w, float r, float echelle) {
  float u = w.x / max(1e-4, r);
  if (u >= 1.0) return vec3(0.0);
  float k = 1.0 - u * u;
  return vec3(k * k, -4.0 * u * k * echelle / max(1e-4, r) * w.zw);
}
`;

// Le champ d'ordre de combustion et son seuil — le cœur du dispositif.
const CHAMP = `
uniform float uRatioCadre, uProgress;
uniform float uFoyerX, uFoyerY, uBordure;
uniform float uEchelle, uDesordre, uDetail, uBulles, uEchelleBulles;
uniform float uBraise, uRoussi, uLargeurChaleur, uMagma, uCombustion;
uniform float uBaseMin, uBaseAmpl;   // bornes mesurées côté JS (cf. bornesChamp)

// Demi-dimensions de la carte dans le repère ISOTROPE : x s'étend sur le ratio,
// y sur 1. Sans ça le bruit serait étiré sur le petit côté et les flammes
// prendraient la forme du cadre.
vec2 demiCarte() { return vec2(uRatioCadre, 1.0) * 0.5; }

// ⚠️ RECOPIÉ À L'IDENTIQUE en JS dans \`bornesChamp()\`, qui mesure les bornes de
// ce champ pour le ramener à [0,1]. Toute modification ici doit y être reportée.
//
// Partie LISSE du champ : la distance à la source. Deux sources possibles — le
// foyer (une tache) et le pourtour (la pellicule qui se rétracte par ses bords).
// Le décalage ±uBordure les fait basculer de l'une à l'autre : à 0 le pourtour
// est repoussé d'un tour complet et c'est le foyer qui prend, à 1 l'inverse.
float sourceLisse(vec2 p) {
  vec2 demi  = demiCarte();
  vec2 foyer = vec2(uFoyerX, uFoyerY) * demi;
  float portee = max(1e-3, length(abs(foyer) + demi));
  float dFoyer = length(p - foyer) / portee;
  // Tchebychev, pas euclidien : le front longe le cadre au lieu d'en arrondir
  // les angles. Une pellicule brûle par ses quatre bords, pas en médaillon.
  float dBord = 1.0 - max(abs(p.x) / demi.x, abs(p.y) / demi.y);
  return minDoux(dFoyer + uBordure, dBord + (1.0 - uBordure), 0.25);
}

// Ordre de combustion : 0 = brûle en premier, 1 = en dernier. FIXE dans le
// temps — seul le seuil bouge. \`cell\` ressort le bouillonnement, que le
// fragment réutilise pour texturer la cendre et l'irisation : ce Worley coûte
// la moitié du champ, le calculer deux fois serait un gâchis.
// Champ LISSE normalisé : la même base, sans une once de bruit. Ramené à [0,1]
// par les bornes mesurées — sans cette normalisation, déplacer le foyer
// re-chronométrerait toute l'animation en douce (le seuil balaierait
// l'essentiel du champ dans le premier tiers, puis ne trouverait plus rien).
//
// C'est le champ dont se sert le SOMMET, et lui seul. Voir le shader de sommet
// pour la raison : elle est la différence entre du film et des éclats de verre.
float champLisse(vec2 p) {
  return (sourceLisse(p) - uBaseMin) / max(1e-3, uBaseAmpl);
}

float ordre(vec2 p, out float cell) {
  cell = cellules(p * uEchelleBulles);
  float f = champLisse(p);
  f += (fbm(p * uEchelle, uDetail) - 0.5) * uDesordre;
  f += (cell - 0.5) * uBulles;
  return f;
}
float ordre(vec2 p) { float c; return ordre(p, c); }

// Amorçage. Les bandes s'allument sur les premiers pour-cent au lieu d'être
// déjà là. C'est ce qui permet au seuil de partir PILE au minimum du champ :
// sans ce fondu il faudrait le faire démarrer bien plus bas pour qu'aucun
// liseré ne soit visible à la première image — de quoi loger tout le
// roussissement, soit près d'un quart de l'animation où rien ne se passe.
float amorce() { return smoothstep(0.0, 0.05, uProgress); }

// Seuil courant : il balaie le champ de son minimum à son maximum.
//
// Les bornes sont STATISTIQUES, pas absolues, et les deux ne se traitent pas de
// la même façon.
//
// En bas, il faut être généreux : une borne trop haute laisserait des trous
// dès la première image. Un FBM de trois octaves ne visite pratiquement jamais
// [0,1] en entier (plutôt [0.15, 0.85]), d'où le 0.35 plutôt que 0.5.
//
// En haut, il faut être SERRÉ. Le maximum du champ est atteint en un point
// unique — le plus éloigné de toutes les sources — et rien ne dit que le bruit
// y soit favorable. Majorer largement fait finir la combustion aux quatre
// cinquièmes, le dernier cinquième à consumer du vide : mesuré, 18 % de temps
// mort. La queue de distribution que cette borne serrée laisse passer est
// épongée par le rideau de fin (cf. le fragment), qui n'a alors qu'un confetti
// à effacer.
float seuilBrulure() {
  float bas  = -0.35 * uDesordre - 0.50 * uBulles - 0.01;
  float haut =  1.0 + 0.12 * (uDesordre + uBulles) + 0.02;
  // Le trou n'est pas au front : il est derrière toute l'épaisseur de matière
  // brûlée. Le seuil doit donc balayer cette épaisseur EN PLUS du champ, sinon
  // il reste à la fin une carte entière de charbon que seul le rideau efface —
  // et un fondu sur une carte pleine, c'est exactement ce qu'on ne veut pas.
  // Conversion carte → champ par la pente typique (≈ 3 par unité de carte).
  haut += uMagma * 3.0;
  // \`uCombustion\` est le maître de la brûlure, et le seul moyen de l'éteindre :
  // à 0 le seuil ne quitte jamais son plancher, donc rien ne brûle — la carte
  // ne fait plus que se replier. Il sert autant en production (une sortie qui
  // ne serait qu'un repli) qu'au labo, où l'on ne peut juger un geste qu'en
  // isolant l'autre.
  return mix(bas, haut, uProgress * uCombustion);
}
`;

const VERT = `#version 300 es
precision highp float;

in vec2 aPos;

uniform vec2  uAspect;
uniform float uRetraitBord, uRetraitGlobal, uGondole, uNbPlis, uEchelleGondole;
uniform float uPerspective, uGonflement;   // uMagma vient du bloc CHAMP
uniform float uRepli, uRepliDebut, uRepliCharniere, uRepliRayon, uRepliInclinaison;

out vec2 vUv;
out vec3 vNormal;
// Pente du champ (unités de champ par unité de carte), calculée ici et
// interpolée. Le fragment s'en sert comme MÉTRIQUE : c'est elle qui convertit
// un écart au seuil en distance de carte.
//
// Elle ne peut pas être mesurée au fragment. \`fwidth\` est une dérivée d'ÉCRAN,
// donc elle intègre la déformation du maillage — et le retrait comprime ce
// maillage, si bien que la dérivée de \`vUv\` SAUTE d'un triangle au suivant.
// Toute la métrique héritait de ce saut, et le moindre seuil net (la
// perforation des bulles) redessinait la tessellation en marches d'escalier.
// Calculée au sommet sur le champ lisse, elle varie continûment.
out float vPente;
out float vDos;   // 1 quand c'est le revers du papier qui nous fait face

${BRUITS}
${CHAMP}

const float TAU = 6.283185307;

// Zone qui travaille, en unités de CARTE. Maximale sur le front, éteinte
// au-delà. Nulle en deçà aussi — la matière y est déjà partie, la déformer ne
// se verrait pas.
float chaleur(float dc) {
  float k = 1.0 - smoothstep(0.0, max(0.005, uLargeurChaleur), max(dc, 0.0));
  // Amorçage : un réglage de chaleur plus large que les bandes ferait
  // autrement gondoler la carte dès la première image, avant tout feu.
  return k * smoothstep(0.0, 0.06, uProgress);
}

// ⚠️ TOUT le sommet travaille sur le champ LISSE, jamais sur le champ bruité.
//
// La raison est la seule chose importante de ce shader. Le bruit du champ
// (Worley à l'échelle 9, FBM) a une fréquence que la grille ne peut pas
// représenter, et il entre dans la géométrie par deux portes :
//   — \`normalize(gradient)\` : près des extrema locaux du bruit, le gradient
//     passe par zéro et sa direction bascule d'un sommet au suivant. Deux
//     voisins reculent alors dans des sens opposés et le maillage se déchire
//     en éclats de verre à arêtes droites.
//   — la chaleur, donc le relief, donc les normales : elles se mettent à
//     varier plus vite que la maille et le film se facette.
// Dans les deux cas le rendu bascule dans la 3D des années 90 — la texture
// s'étire sur de grands triangles plats, et aucun réglage de couleur ne
// rattrape ça.
//
// Ce que ça coûte : le gondolement se centre sur le front LISSE plutôt que sur
// le front festonné. Sur une zone chaude large de 0.14, le décalage ne se voit
// pas. Ce que ça rapporte, en plus des facettes en moins : plus un seul Worley
// au sommet.
//
// Le festonnement, lui, reste entier — c'est le fragment qui le peint.

// Le film gondole là où il chauffe. Deux ondulations : l'une PARALLÈLE au front
// (les iso-lignes du champ le suivent), l'autre organique, pour qu'elles ne
// soient pas réglées au compas.
//
// \`pente\` (unités de champ par unité de carte) est mesurée UNE FOIS dans main
// et passée ici : elle varie peu à l'échelle des différences finies, et la
// recalculer à chaque échantillon quadruplerait le coût du sommet.
float relief(vec2 p, float pente) {
  float dc = (champLisse(p) - seuilBrulure()) / pente;   // distance de carte au front

  // INTUMESCENCE. La matière brûlée ne s'aplatit pas, elle GONFLE : les gaz de
  // pyrolyse restent pris dans un charbon visqueux et le soulèvent. C'est le
  // mécanisme des peintures intumescentes, dont l'épaisseur est multipliée en
  // brûlant — et la raison pour laquelle une brûlure de film se lit comme un
  // BOURRELET et non comme une tache. Maximal au milieu du dépôt, il retombe
  // au front (pas encore soulevé) comme au bord du trou (le charbon s'est
  // effondré).
  float dose = clamp(-dc / max(1e-4, uMagma), 0.0, 1.0);
  float bourrelet = uGonflement * sin(dose * 3.14159265) * smoothstep(0.0, 0.06, uProgress);

  float ch = chaleur(dc);
  if (ch < 0.001) return bourrelet;
  float parallele = sin(champLisse(p) * uNbPlis * TAU) * 0.6;
  float organique = (fbm(p * uEchelleGondole + 11.7, 0.8) - 0.5) * 1.6;
  return (parallele + organique) * uGondole * ch + bourrelet;
}

void main() {
  vUv = aPos;

  vec2 forme = vec2(uRatioCadre, 1.0);
  vec2 p = (aPos - 0.5) * forme;

  // Gradient du champ lisse : il pointe vers ce qui brûlera EN DERNIER, donc à
  // l'opposé du feu. Il sert deux fois — sa DIRECTION dit où la matière chaude
  // recule, sa NORME convertit l'écart au seuil en distance de carte.
  float e = 0.008;
  vec2 g = vec2(champLisse(p + vec2(e, 0.0)) - champLisse(p - vec2(e, 0.0)),
                champLisse(p + vec2(0.0, e)) - champLisse(p - vec2(0.0, e)));
  float pente = max(1e-3, length(g) / (2.0 * e));
  vPente = pente;

  float ch = chaleur((champLisse(p) - seuilBrulure()) / pente);

  // Le recul de la matière chauffée — c'est lui, et lui seul, qui fait que les
  // bords se contractent au lieu de rester découpés net.
  //
  // Il se mesure en FRACTION de la zone chaude, jamais en absolu. La raison est
  // dure : \`ch\` tombe de 1 à 0 sur uLargeurChaleur, donc un recul de R comprime
  // la matière d'un facteur R/uLargeurChaleur sur cette distance. Réglé en
  // absolu, R = 0.10 contre une zone de 0.12 écrase la matière à 17 % de sa
  // longueur — le liseré se réduit à un cheveu et le film part en fils. Exprimé
  // en fraction, la compression EST le réglage, et elle ne peut pas dépasser 1.
  p += normalize(g + vec2(1e-4)) * (uRetraitBord * uLargeurChaleur) * ch;

  // Retour au repère du quad, puis contraction d'ensemble.
  vec2 pos = (p / forme) * (1.0 - uProgress * uRetraitGlobal);

  // Normale par différences finies sur le relief — le gondolement ne se lit
  // que si la lumière l'accroche. Elle est ici dans le repère de la FEUILLE ;
  // le repli la fera tourner avec elle.
  float h  = relief(p, pente);
  float hx = relief(p + vec2(e, 0.0), pente) - relief(p - vec2(e, 0.0), pente);
  float hy = relief(p + vec2(0.0, e), pente) - relief(p - vec2(0.0, e), pente);
  vec3 nFeuille = normalize(vec3(-hx / (2.0 * e), -hy / (2.0 * e), 1.0));

  // ── LE REPLI ─────────────────────────────────────────────────────────────
  // On saisit le bord BAS et on le rabat sur le bord haut.
  //
  // Pliure de RAYON FINI, pas charnière : la feuille roule sur un arc de
  // cercle de rayon R, puis la partie libre repart droite, tangente à cet arc.
  // Deux propriétés en découlent, et ce sont les deux qui comptent.
  //   — C'est DÉVELOPPABLE. Chaque point garde son abscisse curviligne \`u\`
  //     sous la pliure, donc le papier ne s'étire ni ne se comprime nulle
  //     part. Une charnière nue le ferait paraître élastique.
  //   — C'est AMPLE. R est l'épaisseur du geste ; à R → 0 on obtient un pli
  //     cassant, à R grand un rouleau qui se déploie.
  // Conséquence à connaître : à 180° le bord bas n'atteint pas tout à fait le
  // bord haut, il lui manque πR. C'est juste — le rouleau consomme du papier.
  float theta = uRepli * 3.14159265 * smoothstep(uRepliDebut, 1.0, uProgress);
  float phi = 0.0;
  float zRepli = 0.0;
  if (theta > 1e-4) {
    float u = uRepliCharniere - pos.y;      // abscisse curviligne sous la pliure
    if (u > 0.0) {
      float R = max(1e-3, uRepliRayon);
      float arc = R * theta;
      if (u < arc) {                        // encore sur l'arc
        phi = u / R;
        pos.y  = uRepliCharniere - R * sin(phi);
        zRepli = R * (1.0 - cos(phi));
      } else {                              // partie libre, droite et tangente
        phi = theta;
        float reste = u - arc;
        pos.y  = uRepliCharniere - R * sin(phi) - reste * cos(phi);
        zRepli = R * (1.0 - cos(phi)) + reste * sin(phi);
      }
    }
  }

  // Le repère local de la feuille tourne de phi autour de l'axe x :
  //   y_feuille → (0,  cos phi, -sin phi)      z_feuille → (0, sin phi, cos phi)
  float cp = cos(phi), sp = sin(phi);
  vNormal = normalize(vec3(nFeuille.x,
                           nFeuille.y * cp + nFeuille.z * sp,
                          -nFeuille.y * sp + nFeuille.z * cp));
  // Passé 90°, c'est le REVERS du papier qui nous fait face.
  vDos = step(0.0, -cp);

  float z = h + zRepli;

  // BASCULEMENT DE LA SCÈNE. Vu de face, un pli à 90° se projette sur une
  // ligne : c'est géométriquement juste et parfaitement illisible — le geste
  // ne se lit plus que comme un recadrage par le bas. Un léger basculement de
  // la feuille autour de l'axe x suffit à donner au rabat une surface visible,
  // et c'est lui qui fait l'AMPLEUR perçue du mouvement. Il ne s'installe
  // qu'avec le repli, pour ne pas incliner une affiche encore à plat.
  float ti = radians(uRepliInclinaison) * smoothstep(uRepliDebut, 1.0, uProgress);
  float ct = cos(ti), st = sin(ti);
  // Le sens compte : c'est celui qui fait REMONTER le rabat vers le bord haut,
  // conformément au geste. Inversé, la feuille se replie vers le bas de l'écran
  // et on ne lit plus qu'un volet qui bascule sous la carte.
  float yb = pos.y * ct + z * st;
  z        = z * ct - pos.y * st;
  pos.y = yb;

  // Fuite manuelle : pas de matrice de projection pour un seul quad. Le relief
  // du gondolement et la hauteur du repli s'y ajoutent — c'est ce qui fait que
  // le rabat grandit en venant vers l'œil. Garde-fou sur le dénominateur, le
  // labo laisse pousser les curseurs loin.
  pos *= 1.0 / max(0.2, 1.0 - z * uPerspective);

  gl_Position = vec4(pos * 2.0 * uAspect, 0.0, 1.0);
}`;

const FRAG = `#version 300 es
precision highp float;

in vec2 vUv;
in vec3 vNormal;
in float vPente;
in float vDos;

uniform sampler2D uTex;
uniform vec2  uAspect, uUvScale, uUvOffset;
uniform float uTemps, uPxParCarte;
uniform float uLumiereX, uLumiereY, uOmbre, uEclat;
uniform float uLueur, uMoucheture, uDelavage, uCloque, uLaitage;
uniform float uEchelleBulle, uSatellites, uBoursouflure, uCraquelures, uVernis;
uniform float uDistorsion, uPerforation, uGranulation, uGrainEchelle;
// uMagma vient du bloc CHAMP
uniform vec3  uCoulCendre, uCoulBraise, uCoulHalo, uCoulCloque, uCoulCaramel;
uniform float uIriIntensite, uIriFresnel, uIriEchelle, uIriDecalage;
uniform float uGrain, uOpacite, uRayon;

out vec4 fragColor;

${BRUITS}
${CHAMP}

const float TAU = 6.283185307;

// Palette cosinus (Inigo Quilez) — un spectre continu en 4 constantes.
vec3 spectre(float t) {
  return 0.5 + 0.5 * cos(TAU * (vec3(1.0) * t + vec3(0.0, 0.33, 0.67)));
}

// Distance signée à un rectangle arrondi — reproduit le border-radius, que le
// canvas ne peut pas hériter en CSS pendant l'animation.
float boiteArrondie(vec2 p, vec2 demi, float r) {
  vec2 q = abs(p) - demi + r;
  return length(max(q, 0.0)) + min(max(q.x, q.y), 0.0) - r;
}

float luma(vec3 c) { return dot(c, vec3(0.299, 0.587, 0.114)); }

void main() {
  vec2 forme = vec2(uRatioCadre, 1.0);
  vec2 p = (vUv - 0.5) * forme;

  float cell;
  float f = ordre(p, cell);

  // L'écart au seuil, converti en DISTANCE DE CARTE par la pente du champ.
  // Sans cette division, les largeurs de bandes sont exprimées dans un champ
  // dont la pente varie du simple au triple selon l'endroit : le liseré se
  // réduit à un cheveu là où le front est raide et bave là où il est mou.
  // C'est la différence entre une brûlure et un masque animé.
  //
  // La pente vient du SOMMET (cf. vPente) et non de \`fwidth\` : une dérivée
  // d'écran saute à chaque triangle dès que le maillage est comprimé.
  float pente = max(1e-4, vPente);
  float d = (f - seuilBrulure()) / pente;
  // Deux pixels, en unités de carte. Un seul pixel donnerait un contour net —
  // mais le champ porte du bruit à haute fréquence, dont la transition tombe
  // par endroits sous le pixel : c'est là que l'escalier se voit. Deux pixels
  // coûtent un contour à peine plus mou et l'effacent.
  float aa = 2.0 / max(1.0, uPxParCarte);

  // (0,0) = coin haut-gauche de l'affiche, puis recadrage « cover ».
  vec2 uv = vec2(vUv.x, 1.0 - vUv.y) * uUvScale + uUvOffset;
  vec3 base = texture(uTex, uv).rgb;

  // ── L'image meurt avant de disparaître ──
  // Délavage global : la chaleur mange les couleurs bien avant la matière.
  base = mix(base, mix(vec3(luma(base)), vec3(0.84, 0.89, 0.97), 0.5),
             uDelavage * uProgress);

  // Le REVERS du papier. Passé 90° de repli, il n'y a pas d'image de ce côté —
  // sans ça le rabat montre l'affiche à l'endroit et le pli se lit comme un
  // simple glissement, pas comme une feuille retournée. Un rien de l'image
  // transparaît quand même : du papier d'affiche n'est jamais tout à fait opaque.
  base = mix(base, vec3(0.78, 0.76, 0.71) * (0.86 + 0.28 * luma(base)), vDos);

  // Les bandes s'empilent depuis le front (d = 0) vers l'extérieur, c'est-à-dire
  // vers ce qui n'a pas encore brûlé. La matière brûlée, elle, est de l'autre
  // côté — cf. plus bas.
  float b1 = uBraise;          // fin de l'ourlet incandescent
  float b2 = b1 + uCloque;     // fin de la cloque laiteuse
  float b3 = b2 + uRoussi;     // fin du roussissement

  float amo = amorce();

  // Roussissement, le plus en avant : un voile chaud qui assombrit à peine.
  float roussi = smoothstep(b3, b2, d) * amo;
  base = mix(base, base * (uCoulHalo * 1.35 + 0.16) * 0.84, roussi * 0.6);

  // ── Cloque : l'émulsion cuite BLANCHIT avant de partir ──
  // C'est la signature du film brûlé, et ce qui manque le plus quand on
  // transpose un modèle de PAPIER : le papier fonce en roussissant, la gélatine
  // se décolore et vire au laiteux — plus CLAIRE que l'image. Sur les
  // références, cette zone est la plus large de toutes, et c'est elle qui donne
  // le rendu photographique : la brûlure s'y détache en faible contraste au
  // lieu de trancher sur une image restée saturée.
  float cloq = smoothstep(b2, b1, d) * amo;
  vec3 lait = mix(vec3(luma(base)), uCoulCloque, 0.70);
  // La gélatine diffuse : elle ne garde ni les noirs ni les blancs.
  lait = mix(lait, vec3(0.74), 0.30);
  // Marbrure discrète, pas des bulles : les seules bulles franches sont les
  // VIDES de la matière brûlée, et elles sont noires.
  lait += (cell - 0.45) * 0.10;
  base = mix(base, lait, cloq * uLaitage);

  vec3 col = base;

  vec3 N = normalize(vNormal);
  vec3 L = normalize(vec3(uLumiereX, uLumiereY, 1.0));
  vec3 V = vec3(0.0, 0.0, 1.0);
  vec3 H = normalize(L + V);

  // ═══ LA MATIÈRE BRÛLÉE ═══════════════════════════════════════════════════
  // Au-delà du front, ce n'est PAS un trou : c'est une matière. Un dépôt de
  // résine figée, boursouflé, vitrifié, criblé de bulles. Le trou ne vient
  // qu'après, quand ce charbon a fini de se consumer et tombe.
  //
  // \`dose\` mesure l'avancement de la combustion DANS cette matière : 0 au
  // front (elle vient de prendre), 1 au bord du trou (charbon prêt à tomber).
  // L'épaisseur du dépôt n'est PAS constante : le feu s'attarde ici, file là.
  // Sans cette variation la brûlure se lit comme un beignet — un anneau de
  // largeur régulière au contour bien rond, ce qu'aucune combustion ne produit.
  // C'est aussi elle qui déchiquette le bord du trou, gratuitement.
  float magmaL = uMagma * (0.40 + 1.25 * fbm(p * uEchelle * 1.7 + 5.3, 0.7));

  // GRAIN DE LA MATIÈRE. Deux échelles : des taches (le feu ne mord pas
  // uniformément, la fibre est inégale) et une granulation fine (la suie se
  // dépose par paquets). Le grain module la DOSE elle-même, en amont — donc la
  // couleur, la taille des bulles et le percement en héritent tous.
  //
  // C'est le principal antidote à l'« effet 3D ». Un dégradé analytique
  // parfaitement lisse ne ressemble à aucune matière réelle : c'est la
  // signature d'une surface calculée. Teinter APRÈS coup ne suffit pas, il
  // faut que l'irrégularité entre dans le modèle.
  // Bruit LISSE pour la granulation fine, jamais un hash sur grille : le hash
  // pose des carrés, et à la taille réelle d'une affiche d'éventail (170 px)
  // sa cellule tombe sous le pixel — il crépiterait au lieu de granuler. Un
  // bruit interpolé se sous-échantillonne proprement.
  float taches = fbm(p * uGrainEchelle + 4.2, 0.9);
  float suie   = bruit(p * uGrainEchelle * 4.0 + 19.3);
  float dose = clamp(-d / max(1e-4, magmaL)
                     + (taches - 0.5) * uGranulation * 0.55, 0.0, 1.0);
  float dansMagma = smoothstep(aa, -aa, d) * amo;
  float perce = 0.0;   // perforations : des bulles devenues trous traversants

  if (dansMagma > 0.001) {
    // ── Bulles ──
    // La littérature sur la dévolatilisation des polymères fondus décrit une
    // macro-bulle qui grossit en avalant les micro-bulles nées tout autour
    // d'elle (« blister-promoted bubble growth »). D'où DEUX échelles : les
    // grosses, et les grappes de satellites qui les cernent. Une seule échelle
    // de bruit donne un semis régulier, jamais les grappes des références.

    // DISTORSION. Une bulle ne reste pas ronde : elle grossit dans un fondu qui
    // coule et se presse contre ses voisines. On déforme donc le plan AVANT de
    // l'échantillonner, d'une amplitude qui croît avec la dose — la forme
    // ÉVOLUE dans le temps au lieu d'être figée. Sans ça la matière ressemble à
    // une pâte à grumeaux : des blobs qui grossissent sans jamais devenir
    // autre chose.
    vec2 pb = p + (vec2(bruit(p * uEchelleBulle * 0.55 + 3.1),
                        bruit(p * uEchelleBulle * 0.55 + 17.9)) - 0.5)
                  * (uDistorsion / max(1.0, uEchelleBulle)) * (0.35 + dose);

    float aG, aS;
    vec4  wG = worley(pb * uEchelleBulle, aG);

    // CHRONOLOGIE PROPRE À CHAQUE BULLE. Elles ne nucléent pas toutes en même
    // temps, et leur paroi ne cède pas toutes au même moment. Chacune a donc sa
    // naissance et son seuil de rupture, tirés de son propre aléa — sans cette
    // désynchronisation, tout le champ de bulles respire d'un seul bloc.
    float naissance = aG * 0.45;
    float age = clamp((dose - naissance) / max(0.05, 0.92 - naissance), 0.0, 1.0);

    // Rayon propre à CHAQUE bulle, du simple au triple. C'est ce tirage qui
    // sépare une mousse d'un papier bulle.
    float rG = 0.46 * smoothstep(0.0, 0.62, age) * (0.40 + 1.15 * aG);
    vec3  bG = bosse(wG, rG, uEchelleBulle);

    // TROIS ÉTATS QUI S'ENCHAÎNENT : dôme → cratère → trou.
    // La bulle grossit, sa paroi s'amincit, elle s'affaisse, puis elle ROMPT et
    // devient un vide traversant. C'est ce dernier passage qui manquait : des
    // grumeaux qui ne deviennent jamais rien restent des grumeaux.
    // \`uPerforation\` abaisse le SEUIL de rupture, il ne dose pas l'ouverture :
    // une paroi qui cède, cède. À 0 aucune ne rompt (le seuil passe au-dessus
    // de l'âge maximal), à 1 la plupart rompent, et tôt. Le tirage garde
    // l'étalement — certaines bulles tiennent jusqu'au bout.
    float seuilR = mix(1.60, 0.30,
                       uPerforation * (0.35 + 0.65 * hash21(vec2(aG * 31.7, 3.1))));
    float ouvert = smoothstep(seuilR, seuilR + 0.30, age);

    // Affaissement : le dôme passe de +1 à −1, la lumière le prend à
    // contre-pente et il se lit comme un cratère.
    float affaisse = smoothstep(0.0, 0.55, ouvert);
    bG.x  *= 1.0 - 2.0 * affaisse;
    bG.yz *= 1.0 - 2.0 * affaisse;

    // Puis la rupture proprement dite : le cœur de la bulle s'ouvre.
    // Seuils volontairement LARGES : une rupture franche découperait le trou au
    // rasoir, et un bord net posé sur \`vUv\` redessine la tessellation dès que
    // le maillage est comprimé. Un bord mou coûte un peu de netteté et efface
    // l'escalier.
    perce = smoothstep(0.40, 1.0, ouvert)
          * smoothstep(0.95, 0.30, wG.x / max(1e-4, rG));

    // Les satellites vivent AUTOUR des grosses, pas au travers : on les
    // cantonne à la couronne qui borde chaque macro-bulle.
    float couronne = smoothstep(0.75, 1.25, wG.x / max(1e-4, rG));
    vec4  wS = worley(pb * uEchelleBulle * 3.3 + 31.7, aS);
    float ageS = clamp((dose - aS * 0.35) / 0.8, 0.0, 1.0);
    vec3  bS = bosse(wS, 0.40 * smoothstep(0.0, 0.5, ageS) * (0.35 + 1.2 * aS),
                     uEchelleBulle * 3.3)
             * uSatellites * couronne;

    float haut = bG.x + bS.x;
    vec2  pente = bG.yz + bS.yz;

    // Normale de la matière, calculée AU FRAGMENT. Une bulle fait deux à huit
    // pixels, une maille de la grille en fait trente : les mettre dans la
    // géométrie demanderait une grille cent fois plus fine, pour un relief
    // qu'on ne perçoit de toute façon que par sa brillance.
    vec3 Nb = normalize(vec3(-pente * uBoursouflure, 1.0));

    // ── Couleur ──
    // Le chemin colorimétrique est celui mesuré sur la cellulose chauffée :
    // L* décroît de façon monotone pendant que a* et b* montent. Autrement
    // dit clair → jaune → ambre → brun rouge → noir, dans cet ordre et sans
    // raccourci. C'est pour ça qu'un dégradé à deux teintes ne ressemble
    // jamais à une brûlure : il saute la moitié du trajet.
    // Seuils resserrés vers le bas : sur les références, l'ambre n'est qu'un
    // liseré au contact de l'image intacte, et c'est le brun-charbon qui occupe
    // l'essentiel de la surface. Une rampe étalée régulièrement donne une
    // croûte de pain — trop claire, trop douce, trop égale.
    float t = dose;
    vec3 mat = mix(uCoulHalo,   uCoulCaramel, smoothstep(0.00, 0.16, t));
    mat      = mix(mat,         uCoulBraise,  smoothstep(0.13, 0.40, t));
    mat      = mix(mat,         uCoulCendre,  smoothstep(0.34, 0.82, t));

    // Les bulles sont des VIDES, et rien d'autre. Une bulle claire se lit comme
    // une perle POSÉE sur la matière — c'est un objet éclairé, pas un creux, et
    // c'était le premier responsable de l'air « rendu 3D ». Un vide n'a pas de
    // couleur propre : il absorbe. Dôme ou cratère, on assombrit.
    mat *= 1.0 - abs(haut) * 0.85;

    // Grain. Les taches d'abord (le feu ne mord pas uniformément), la
    // granulation ensuite (la suie se dépose par paquets). Elles cassent le
    // dégradé analytique, qui est ce qui trahit une surface calculée.
    mat *= mix(1.0, 0.55 + 0.9 * taches, uGranulation);
    mat *= mix(1.0, 0.72 + 0.56 * suie,  uGranulation);

    // Craquelures : le charbon intumescent se fend sur les parois minces entre
    // les vides. F2 − F1 s'annule exactement là.
    float fissure = smoothstep(0.09, 0.0, wG.y - wG.x) * smoothstep(0.5, 0.95, dose);
    mat = mix(mat, mat * 0.22, fissure * uCraquelures);

    // ── Vernis ──
    // La résine figée brille, là où l'affiche est mate. Mais un spéculaire
    // ANALYTIQUE sur une normale lisse produit une tache parfaite et continue —
    // la signature même du rendu 3D. On l'élargit (exposant bas) et on le
    // hache par la granulation : une micro-rugosité éclate un reflet en
    // pointillé, c'est ce que fait toute vraie surface.
    mat *= 0.62 + 0.55 * (dot(Nb, L) * 0.5 + 0.5);
    mat += pow(max(dot(Nb, H), 0.0), 18.0) * uVernis
         * mix(1.0, 0.15 + 1.7 * suie, uGranulation) * 0.45;

    col = mix(col, mat, dansMagma);
  }

  // Ourlet incandescent, sur le front lui-même. Gaussienne et non fenêtre :
  // aucun bord dur, la lueur se fond des deux côtés. Il REMPLACE d'abord, il
  // n'ajoute qu'ensuite — c'est de la matière portée au rouge, pas un halo posé
  // par-dessus. Purement additif sur une émulsion blanchie il sature à blanc,
  // et cerne la brûlure d'un néon.
  float eb = (d - uBraise * 0.5) / max(1e-4, uBraise * 0.9);
  vec3  feu = mix(uCoulCaramel, vec3(1.0), 0.45);
  float mBraise = exp(-eb * eb) * amo;
  col  = mix(col, feu, mBraise * 0.75);
  col += feu * mBraise * uLueur * 0.30;

  // Mouchetures : l'argent métallique de l'émulsion, qui reste quand la
  // gélatine est partie. Un disque DOUX centré dans sa cellule, pas un \`step\`
  // sur la grille — le step donnait littéralement des pixels carrés.
  vec2 gp = p * 130.0;
  vec2 fp = fract(gp) - 0.5;
  float pointe = smoothstep(0.40, 0.14, length(fp))
               * step(1.0 - uMoucheture * 0.3, hash21(floor(gp)));
  col *= 1.0 - pointe * smoothstep(b2, -magmaL * 0.5, d) * 0.55;

  // ── Lumière sur le film gondolé ──
  // Éclairage enveloppant, normalisé pour culminer à 1 : le modelé ne fait
  // qu'OMBRER, il ne délave pas l'affiche. Il s'efface dans la matière brûlée,
  // qui porte déjà le sien : l'appliquer deux fois écraserait le charbon.
  float diff = pow(dot(N, L) * 0.5 + 0.5, 1.3);
  col *= mix(mix(1.0, 0.55 + 0.45 * diff, uOmbre), 1.0, dansMagma);
  col += pow(max(dot(N, H), 0.0), 24.0) * uEclat * (1.0 - dansMagma);

  // Irisation de la gélatine cuite — le voile d'huile des références. Cantonnée
  // à la matière FRAÎCHEMENT brûlée : plus loin, il n'y a plus de gélatine, il
  // n'y a que du charbon. Et désaturée de moitié : la palette cosinus parcourt
  // toutes les teintes, or une brûlure n'a ni vert ni magenta franc.
  if (uIriIntensite > 0.001) {
    float fres = pow(1.0 - abs(dot(N, V)), uIriFresnel);
    float ti = fres * uIriEchelle + cell * 0.9 + uIriDecalage;
    vec3 iri = spectre(ti);
    iri = mix(vec3(luma(iri)), iri, 0.5);
    col += iri * uIriIntensite * dansMagma * smoothstep(0.55, 0.05, dose) * (0.5 + fres);
  }

  // Grain — casse le lissé du dégradé, comme sur une impression.
  col += (hash21(vUv * 512.0 + uTemps) - 0.5) * uGrain;

  // ── Masque ──
  vec2 q = (vUv - 0.5) * 2.0 * uAspect;
  float dm = boiteArrondie(q, uAspect, uRayon * min(uAspect.x, uAspect.y) * 2.0);
  float am = fwidth(dm) * 1.5;
  float masque = 1.0 - smoothstep(-am, am, dm);

  // Le trou. Il n'est PAS au front : il est derrière toute l'épaisseur de
  // matière brûlée, là où le charbon a fini de se consumer et s'est détaché.
  // Les bulles rompues percent la matière de part en part : la brûlure se
  // change en dentelle avant de céder tout à fait, au lieu de reculer en
  // bloc. C'est ce qui donne son âge à la matière.
  float trou = 1.0 - smoothstep(-magmaL - aa, -magmaL + aa, d);
  trou = max(trou, perce);

  // Rideau de fin. La borne haute du seuil est serrée exprès (cf. seuilBrulure)
  // et laisse donc passer la queue de distribution du bruit : un confetti oublié
  // sur un écran vide se voit plus qu'une brûlure ratée. Le rideau ne mord que
  // sur le dernier dixième, là où il ne reste au plus que ce confetti — assez
  // peu pour qu'on ne lise pas un fondu.
  float fin = 1.0 - smoothstep(0.90, 1.0, uProgress);

  fragColor = vec4(col, masque * uOpacite * (1.0 - trou) * fin);
}`;

// ── Plomberie WebGL ────────────────────────────────────────────────────────
// Décalquée de cloth.js, à un détail près : les uniformes sont téléversés
// GÉNÉRIQUEMENT depuis l'objet de paramètres (`nbPlis` → `uNbPlis`), au lieu
// d'une liste tenue à la main. Trente-cinq réglages, c'est le seuil où la liste
// manuelle commence à coûter un oubli silencieux à chaque ajout.

function compiler(gl, type, source) {
  const sh = gl.createShader(type);
  gl.shaderSource(sh, source);
  gl.compileShader(sh);
  if (!gl.getShaderParameter(sh, gl.COMPILE_STATUS)) {
    const log = gl.getShaderInfoLog(sh);
    gl.deleteShader(sh);
    throw new Error('Shader : ' + log);
  }
  return sh;
}

function programme(gl) {
  const p = gl.createProgram();
  const vs = compiler(gl, gl.VERTEX_SHADER, VERT);
  const fs = compiler(gl, gl.FRAGMENT_SHADER, FRAG);
  gl.attachShader(p, vs);
  gl.attachShader(p, fs);
  gl.linkProgram(p);
  if (!gl.getProgramParameter(p, gl.LINK_STATUS)) {
    throw new Error('Link : ' + gl.getProgramInfoLog(p));
  }
  gl.deleteShader(vs);
  gl.deleteShader(fs);
  return p;
}

// Grille indexée en [0,1]². `n` subdivisions par côté → n² quads.
function grille(n) {
  const pts = new Float32Array((n + 1) * (n + 1) * 2);
  let k = 0;
  for (let y = 0; y <= n; y++) {
    for (let x = 0; x <= n; x++) {
      pts[k++] = x / n;
      pts[k++] = y / n;
    }
  }
  const idx = new Uint32Array(n * n * 6);
  let j = 0;
  for (let y = 0; y < n; y++) {
    for (let x = 0; x < n; x++) {
      const a = y * (n + 1) + x, b = a + 1, c = a + (n + 1), d = c + 1;
      idx[j++] = a; idx[j++] = c; idx[j++] = b;
      idx[j++] = b; idx[j++] = c; idx[j++] = d;
    }
  }
  return { pts, idx };
}

const nomUniforme = cle => 'u' + cle[0].toUpperCase() + cle.slice(1);

// ⚠️ Doit rester le DÉCALQUE EXACT de `minDoux` / `sourceLisse` dans le shader.
const minDoux = (a, b, k) => {
  const h = Math.min(1, Math.max(0, 0.5 + (0.5 * (b - a)) / k));
  return b + (a - b) * h - k * h * (1 - h);
};

/**
 * Bornes de la partie LISSE du champ, mesurées sur une grille.
 *
 * Le shader s'en sert pour ramener le champ à [0,1]. Pourquoi mesurer plutôt
 * que calculer : la source est un minimum doux entre deux distances dont les
 * maxima ne sont pas au même endroit — son maximum réel n'a pas de forme close,
 * et le majorer à 1 revient à donner au seuil une course qu'il ne parcourt pas.
 * Résultat sans ça : l'affiche est aux trois quarts consumée à 35 %, puis il ne
 * se passe plus rien. 41×41 évaluations de deux distances, à chaque `regler()`.
 */
function bornesChamp(P) {
  const dx = P.ratioCadre * 0.5, dy = 0.5;
  const fx = P.foyerX * dx, fy = P.foyerY * dy;
  const portee = Math.max(1e-3, Math.hypot(Math.abs(fx) + dx, Math.abs(fy) + dy));
  let mn = Infinity, mx = -Infinity;
  const N = 40;
  for (let j = 0; j <= N; j++) {
    const y = (j / N - 0.5) * 2 * dy;
    for (let i = 0; i <= N; i++) {
      const x = (i / N - 0.5) * 2 * dx;
      const dFoyer = Math.hypot(x - fx, y - fy) / portee;
      const dBord = 1 - Math.max(Math.abs(x) / dx, Math.abs(y) / dy);
      const v = minDoux(dFoyer + P.bordure, dBord + (1 - P.bordure), 0.25);
      if (v < mn) mn = v;
      if (v > mx) mx = v;
    }
  }
  return { min: mn, ampl: Math.max(1e-3, mx - mn) };
}

/**
 * Crée un renderer de combustion sur un <canvas>.
 * Renvoie `null` si WebGL2 est indisponible — l'appelant retombe alors sur
 * l'affichage <img> classique.
 */
export function creerBrulure(canvas, params = {}) {
  const gl = canvas.getContext('webgl2', {
    alpha: true, antialias: true, premultipliedAlpha: false,
  });
  if (!gl) return null;

  let P = { ...BRULURE_DEFAUTS, ...params };
  let prog;
  try {
    prog = programme(gl);
  } catch (e) {
    console.warn('[brulure] shader non compilé', e);
    return null;
  }

  const vao = gl.createVertexArray();
  gl.bindVertexArray(vao);
  const vbo = gl.createBuffer();
  const ibo = gl.createBuffer();
  let nbIndices = 0;

  function construireGrille(n) {
    const { pts, idx } = grille(n);
    gl.bindBuffer(gl.ARRAY_BUFFER, vbo);
    gl.bufferData(gl.ARRAY_BUFFER, pts, gl.STATIC_DRAW);
    const loc = gl.getAttribLocation(prog, 'aPos');
    gl.enableVertexAttribArray(loc);
    gl.vertexAttribPointer(loc, 2, gl.FLOAT, false, 0, 0);
    gl.bindBuffer(gl.ELEMENT_ARRAY_BUFFER, ibo);
    gl.bufferData(gl.ELEMENT_ARRAY_BUFFER, idx, gl.STATIC_DRAW);
    nbIndices = idx.length;
  }
  construireGrille(P.subdivisions);

  // Emplacements : ceux dérivés des paramètres, plus les quelques uniformes
  // composés que le JS calcule (aspect, recadrage, temps, progression).
  const U = {};
  for (const cle of Object.keys(BRULURE_DEFAUTS)) {
    if (HORS_SHADER.has(cle)) continue;
    U[cle] = gl.getUniformLocation(prog, nomUniforme(cle));
  }
  for (const nom of ['uProgress', 'uAspect', 'uUvScale', 'uUvOffset', 'uTemps', 'uTex',
                     'uBaseMin', 'uBaseAmpl', 'uPxParCarte']) {
    U[nom] = gl.getUniformLocation(prog, nom);
  }

  // Bornes du champ : elles ne dépendent que du foyer, du pourtour et du ratio.
  // On les recalcule à chaque `regler()` plutôt que de pister ces trois clés —
  // 1681 itérations, contre le risque d'un réglage qui ne se répercute pas.
  let bornes = bornesChamp(P);

  const tex = gl.createTexture();
  gl.bindTexture(gl.TEXTURE_2D, tex);
  // Pixel gris le temps du chargement — évite un flash noir au premier rendu.
  gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA, 1, 1, 0, gl.RGBA, gl.UNSIGNED_BYTE,
    new Uint8Array([32, 32, 32, 255]));

  gl.enable(gl.BLEND);
  gl.blendFuncSeparate(gl.SRC_ALPHA, gl.ONE_MINUS_SRC_ALPHA, gl.ONE, gl.ONE_MINUS_SRC_ALPHA);

  let ratioImage = 2 / 3;      // largeur / hauteur de l'affiche
  let progress = 0;
  let temps = 0;
  let raf = null;
  let debut = 0;
  let easing = resoudreEasing('lineaire');
  let onFin = null;
  let detruit = false;

  function dimensionner() {
    const dpr = Math.min(window.devicePixelRatio || 1, 2);
    const w = Math.max(1, Math.round(canvas.clientWidth * dpr));
    const h = Math.max(1, Math.round(canvas.clientHeight * dpr));
    if (canvas.width !== w || canvas.height !== h) {
      canvas.width = w;
      canvas.height = h;
    }
    gl.viewport(0, 0, canvas.width, canvas.height);
  }

  // Le quad prend la forme du CADRE, inscrit dans le canvas à l'échelle
  // 1/débordement — la marge restante laisse le film gondolé sortir du cadre
  // sans être coupé.
  function aspect() {
    const cw = canvas.clientWidth || 1, ch = canvas.clientHeight || 1;
    const dispo = cw / ch;
    const rc = P.ratioCadre;
    let ax, ay;
    if (rc > dispo) { ax = 1; ay = dispo / rc; }   // limité par la largeur
    else            { ay = 1; ax = rc / dispo; }   // limité par la hauteur
    const k = 1 / Math.max(1e-3, P.debordement);
    return [ax * k, ay * k];
  }

  // Recadrage « cover » + `object-position:top` — réplique exacte de la règle
  // CSS de .ev-card img. Le quad ne bouge pas, seule la fenêtre de texture change.
  function uvCover() {
    const rc = P.ratioCadre, ri = ratioImage;
    if (ri > rc) {                       // affiche plus large que le cadre
      const s = rc / ri;                 // → on rogne à gauche/droite, centré
      return { scale: [s, 1], offset: [(1 - s) / 2, 0] };
    }
    const s = ri / rc;                   // affiche plus haute → on garde le HAUT
    return { scale: [1, s], offset: [0, 0] };
  }

  function dessiner() {
    if (detruit) return;
    dimensionner();
    gl.clearColor(0, 0, 0, 0);
    gl.clear(gl.COLOR_BUFFER_BIT);
    gl.useProgram(prog);
    gl.bindVertexArray(vao);

    const [ax, ay] = aspect();
    const uv = uvCover();
    gl.uniform1f(U.uProgress, progress);
    gl.uniform2f(U.uAspect, ax, ay);
    gl.uniform2f(U.uUvScale, uv.scale[0], uv.scale[1]);
    gl.uniform2f(U.uUvOffset, uv.offset[0], uv.offset[1]);
    gl.uniform1f(U.uTemps, temps);
    gl.uniform1f(U.uBaseMin, bornes.min);
    gl.uniform1f(U.uBaseAmpl, bornes.ampl);
    // Pixels de sortie que couvre UNE unité de carte (la hauteur du cadre) : le
    // quad occupe la fraction `ay` de la hauteur du canvas.
    gl.uniform1f(U.uPxParCarte, Math.max(1, ay * canvas.height));

    for (const cle of Object.keys(BRULURE_DEFAUTS)) {
      if (HORS_SHADER.has(cle)) continue;
      if (COULEURS.has(cle)) {
        const [r, v, b] = hexVersRvb(P[cle]);
        gl.uniform3f(U[cle], r, v, b);
      } else {
        gl.uniform1f(U[cle], P[cle]);
      }
    }

    gl.activeTexture(gl.TEXTURE0);
    gl.bindTexture(gl.TEXTURE_2D, tex);
    gl.uniform1i(U.uTex, 0);

    gl.drawElements(gl.TRIANGLES, nbIndices, gl.UNSIGNED_INT, 0);
  }

  function boucle(ts) {
    if (detruit) return;
    if (!debut) debut = ts;
    const ecoule = ts - debut - P.retard;
    temps = (ts - debut) / 1000;
    if (ecoule < 0) {
      progress = 0;
    } else {
      const brut = Math.min(1, ecoule / Math.max(1, P.duree));
      progress = easing(brut);
      if (brut >= 1) {
        dessiner();
        raf = null;
        if (onFin) { const f = onFin; onFin = null; f(); }
        return;
      }
    }
    dessiner();
    raf = requestAnimationFrame(boucle);
  }

  return {
    /** Charge l'affiche. Rejette si l'image n'est pas exploitable (CORS, 404). */
    charger(src) {
      return new Promise((resolve, reject) => {
        const img = new Image();
        // ⚠️ Doit être posé AVANT `src`, sinon la requête part sans CORS.
        img.crossOrigin = 'anonymous';
        img.onload = () => {
          if (detruit) return reject(new Error('détruit'));
          ratioImage = img.naturalWidth / img.naturalHeight || 2 / 3;
          gl.bindTexture(gl.TEXTURE_2D, tex);
          gl.pixelStorei(gl.UNPACK_FLIP_Y_WEBGL, false);
          try {
            gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA, gl.RGBA, gl.UNSIGNED_BYTE, img);
          } catch (e) {
            // Image « tainted » : l'origine n'autorise pas le partage.
            return reject(e);
          }
          gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.LINEAR_MIPMAP_LINEAR);
          gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.LINEAR);
          gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE);
          gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE);
          gl.generateMipmap(gl.TEXTURE_2D);
          dessiner();
          resolve({ largeur: img.naturalWidth, hauteur: img.naturalHeight });
        };
        img.onerror = () => reject(new Error('image non chargée : ' + src));
        img.src = src;
      });
    },

    /**
     * Consume l'affiche, de l'intacte au vide.
     * `courbe` : nom, [x1,y1,x2,y2] ou fonction. Une courbe qui dépasse 1
     * n'apporte rien ici — il n'y a rien au-delà du vide.
     */
    jouer(courbe = 'lineaire', fin = null) {
      easing = resoudreEasing(courbe);
      onFin = fin;
      debut = 0;
      progress = 0;
      if (raf) cancelAnimationFrame(raf);
      raf = requestAnimationFrame(boucle);
    },

    /** Fige l'animation et positionne la progression à la main (scrub). */
    figer(p) {
      if (raf) { cancelAnimationFrame(raf); raf = null; }
      progress = Math.min(1, Math.max(0, p));
      dessiner();
    },

    /** Met à jour les paramètres à chaud. Reconstruit la grille si besoin. */
    regler(patch) {
      const avant = P.subdivisions;
      P = { ...P, ...patch };
      bornes = bornesChamp(P);
      if (P.subdivisions !== avant) construireGrille(P.subdivisions);
      if (!raf) dessiner();
    },

    parametres() { return { ...P }; },
    progression() { return progress; },
    redessiner() { dessiner(); },

    detruire() {
      detruit = true;
      if (raf) cancelAnimationFrame(raf);
      gl.deleteTexture(tex);
      gl.deleteBuffer(vbo);
      gl.deleteBuffer(ibo);
      gl.deleteVertexArray(vao);
      gl.deleteProgram(prog);
      gl.getExtension('WEBGL_lose_context')?.loseContext();
    },
  };
}
