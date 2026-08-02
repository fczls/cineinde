// ═══════════════════════════════════════════════════════════════════════════
// cloth.js — dépliage « tissu » d'une affiche, en WebGL2.
//
// Module PARTAGÉ entre le labo de réglage (design/lab/cloth.html) et la
// production (assemblé dans index.html par build_ui.py). C'est le point clé du
// dispositif : on règle au labo EXACTEMENT le code qui tourne en prod, et le
// transfert se fait par un objet de paramètres — jamais par report de code.
//
// Pourquoi du WebGL2 brut plutôt que three.js / R3F : la scène est UN quad
// texturé. Tout l'intérêt tient dans le shader, identique quelle que soit la
// couche au-dessus ; une lib de scène 3D coûterait ici 150 à 200 Ko pour ne
// remplacer qu'une centaine de lignes de plomberie.
//
// ⚠️ CORS : une image cross-origin sans en-tête `Access-Control-Allow-Origin`
// ne peut pas être uploadée en texture — WebGL lève une erreur de sécurité, ce
// n'est pas un simple avertissement. `charger()` échoue proprement dans ce cas
// et l'appelant doit retomber sur l'affichage <img> classique.
// ═══════════════════════════════════════════════════════════════════════════

// ── Réglages par défaut ────────────────────────────────────────────────────
// Toute valeur exposée au labo vit ici. Un preset exporté par le labo est un
// sous-ensemble de cet objet : `{...CLOTH_DEFAUTS, ...preset}` suffit à rejouer.
export const CLOTH_DEFAUTS = {
  // — Temps —
  duree:          2400,   // ms, durée du dépliage complet
  retard:         0,      // ms avant démarrage (sert au décalage entre cartes)
  // Amplification du dépassement de la courbe. Une Bézier cubique ne culmine
  // guère au-delà de 1.1 : sans ce facteur, le pli inversé du rebond resterait
  // sous le seuil du visible. Il traduit un dépassement de TEMPS en amplitude
  // de MATIÈRE — les deux n'ont aucune raison d'être à la même échelle.
  ampliRebond:    2.5,

  // — Géométrie du pli —
  angle:          -35,    // ° — direction des plis (0 = plis verticaux)
  nbPlis:         1.9,    // nombre de plis sur la diagonale
  amplitude:      0.26,   // profondeur du pli à l'état replié (unités de quad)
  arete:          0.10,   // 0 = ondulation douce, 1 = arête franche (= carton)
  desordre:       0.85,   // 0 = onde périodique, 1 = ondulation organique
  derive:         0.55,   // glissement des plis pendant que le tissu se pose
  nbRides:        2.4,    // ondulation secondaire, perpendiculaire aux plis
  amplitudeRides: 0.26,   // poids de cette ondulation secondaire
  // Retrait de l'emprise, DÉRIVÉ de l'amplitude et de la fréquence des plis
  // (conservation de la longueur d'étoffe). 0 = emprise figée (irréaliste),
  // 1 = conservation exacte, >1 = exagéré. Ce n'est pas une taille réglée à la
  // main : plus les plis sont profonds, plus la carte se rétracte d'elle-même.
  retrait:        1.0,
  balayage:       1.10,   // 0 = tout s'aplatit ensemble, 1 = vague qui traverse
  traineBord:     0.55,   // retard des bords et des coins (ils se posent en dernier)
  chute:          0.05,   // hauteur d'où le tissu descend, en unités de quad
  approche:       0.06,   // sur-taille initiale (échelle plane, sans profondeur)
  // Décalage en Z de la carte ENTIÈRE pendant le vol, résorbé en se posant.
  // > 0 : elle arrive en avant (plus près) ; < 0 : en retrait (plus loin).
  // Contrairement à `approche`, il passe par la division perspective : c'est
  // une vraie profondeur, qui se compose avec le relief des plis.
  profondeurZ:    0.0,
  perspective:    0.32,   // fuite : les plis lointains rétrécissent
  rotation:       5,      // ° d'inclinaison initiale, résorbée au dépliage
  debordement:    1.28,   // marge autour de l'affiche (le pli déborde du cadre)

  // — Lumière —
  lumiereX:       -0.45,
  lumiereY:       0.70,
  ombre:          0.30,   // force du modelé (0 = image plate)
  eclat:          0.16,   // reflet spéculaire sur les crêtes
  translucide:    0.22,   // lumière traversant le tissu là où il se détourne

  // — Iridescence —
  iriIntensite:   0.42,   // poids global du voile spectral
  iriFresnel:     2.6,    // concentration sur les angles rasants
  iriEchelle:     1.35,   // étalement du spectre
  iriDecalage:    0.10,   // rotation de la teinte
  iriVitesse:     0.25,   // dérive de la teinte dans le temps
  iriRepos:       0.03,   // voile résiduel une fois le tissu à plat

  // — Matière —
  grain:          0.030,
  voile:          0.35,   // transparence en vol — le tissu s'opacifie en se posant
  opacite:        1.0,

  // — Rendu —
  subdivisions:   64,     // finesse de la grille (par côté)
  rayon:          0.094,  // coins arrondis, en fraction du petit côté (16/170)

  // Ratio du GABARIT, pas celui de l'affiche. L'éventail impose trois gabarits
  // et recadre chaque affiche dedans (components.css, § Éventail) : le quad a
  // donc toujours la forme du gabarit, et c'est la TEXTURE qu'on recadre —
  // sinon chaque salle dicterait sa géométrie et l'éventail se déformerait.
  ratioCadre:     170 / 254,
};

// Les gabarits de l'éventail, repris de components.css. Un seul format : le
// PORTRAIT — toute affiche y est recadrée, y compris une source paysage.
export const GABARITS = {
  vertical: { l: 170, h: 254 },   // au centre
  cote:     { l: 160, h: 240 },   // réduit, sur les côtés
};

/**
 * Courbe de Bézier cubique façon CSS `cubic-bezier(x1,y1,x2,y2)`, avec P0=(0,0)
 * et P3=(1,1). x1/x2 sont bornés à [0,1] (le temps ne recule pas) mais y1/y2
 * sont LIBRES : c'est en sortant de [0,1] qu'on obtient l'anticipation (y<0)
 * et le rebond (y>1).
 *
 * La courbe est paramétrique : X(s) et Y(s) pour s ∈ [0,1]. On cherche donc le
 * s tel que X(s) = t (Newton-Raphson, bissection en repli quand la dérivée
 * s'aplatit), puis on renvoie Y(s).
 */
export function bezier(x1, y1, x2, y2) {
  x1 = Math.min(1, Math.max(0, x1));
  x2 = Math.min(1, Math.max(0, x2));
  const cx = 3 * x1, bx = 3 * (x2 - x1) - cx, ax = 1 - cx - bx;
  const cy = 3 * y1, by = 3 * (y2 - y1) - cy, ay = 1 - cy - by;
  const X  = s => ((ax * s + bx) * s + cx) * s;
  const Y  = s => ((ay * s + by) * s + cy) * s;
  const dX = s => (3 * ax * s + 2 * bx) * s + cx;

  return function (t) {
    if (t <= 0) return 0;
    if (t >= 1) return 1;          // Y(1) = 1 par construction
    let s = t;
    for (let i = 0; i < 8; i++) {
      const err = X(s) - t;
      if (Math.abs(err) < 1e-6) return Y(s);
      const d = dX(s);
      if (Math.abs(d) < 1e-6) break;
      s -= err / d;
    }
    let lo = 0, hi = 1;
    s = t;
    for (let i = 0; i < 32; i++) {
      s = (lo + hi) * 0.5;
      const x = X(s);
      if (Math.abs(x - t) < 1e-6) break;
      if (x < t) lo = s; else hi = s;
    }
    return Y(s);
  };
}

// Points de départ pour l'éditeur — les quatre courbes nommées, exprimées en
// Bézier pour qu'un preset se retrouve directement sous les poignées.
export const COURBES = {
  sortie:   [0.215, 0.610, 0.355, 1.000],
  douce:    [0.645, 0.045, 0.355, 1.000],
  ressort:  [0.340, 1.560, 0.640, 1.000],
  lineaire: [0.000, 0.000, 1.000, 1.000],
};

// Accepte un nom, un quadruplet [x1,y1,x2,y2] ou une fonction.
export function resoudreEasing(c) {
  if (typeof c === 'function') return c;
  if (Array.isArray(c) && c.length === 4) return bezier(c[0], c[1], c[2], c[3]);
  if (typeof c === 'string' && COURBES[c]) return bezier(...COURBES[c]);
  return EASINGS[c] || EASINGS.sortie;
}

// Deux courbes d'accélération suffisent ici ; le labo les expose au choix.
export const EASINGS = {
  // Décélération franche — la référence du projet (cf. .ev-card).
  sortie:  t => 1 - Math.pow(1 - t, 3),
  douce:   t => t < 0.5 ? 4 * t * t * t : 1 - Math.pow(-2 * t + 2, 3) / 2,
  ressort: t => {
    // Léger dépassement en fin de course : le tissu « claque » à plat.
    const c = 1.70158 * 1.2;
    return 1 + (c + 1) * Math.pow(t - 1, 3) + c * Math.pow(t - 1, 2);
  },
  lineaire: t => t,
};

const TAU = Math.PI * 2;

// ── Shaders ────────────────────────────────────────────────────────────────

const VERT = `#version 300 es
precision highp float;

in vec2 aPos;              // position dans la grille, en [0,1]²

uniform float uProgress;   // 0 = replié, 1 = à plat
uniform vec2  uAspect;     // mise à l'échelle du quad dans le canvas
uniform float uAngle, uNbPlis, uAmplitude, uArete, uDesordre, uDerive, uAmpliRebond;
uniform float uNbRides, uAmplitudeRides, uRetrait, uBalayage, uTraineBord;
uniform float uPerspective, uRotation, uChute, uApproche, uProfondeurZ;

out vec2  vUv;
out vec3  vNormal;
out float vPli;            // activité locale du pli (1 = encore replié)

const float TAU = 6.283185307;

// Onde triangulaire : c'est elle qui donne l'ARÊTE franche du pli, là où une
// sinusoïde ne produit qu'une ondulation molle.
float triangle(float x) {
  return 1.0 - 2.0 * abs(fract(x) - 0.5);
}

vec2 tourne(vec2 p, float a) {
  float c = cos(a), s = sin(a);
  return vec2(p.x * c - p.y * s, p.x * s + p.y * c);
}

// Le dépliage ne se fait pas partout en même temps. Deux retards se cumulent :
// un BALAYAGE directionnel qui traverse la feuille, et une TRAÎNÉE DE BORD qui
// laisse les bords et les coins se poser en dernier — c'est la signature du
// tissu léger, celle qu'un autocollant n'a jamais.
float avancementLocal(vec2 c, vec2 dir) {
  float d01  = clamp(dot(c, dir) + 0.5, 0.0, 1.0);
  float bord = clamp(length(c * vec2(1.0, 1.15)) * 1.9, 0.0, 1.0);
  float retard = d01 * uBalayage + bord * uTraineBord;
  float etale  = 1.0 + uBalayage + uTraineBord;
  return clamp(uProgress * etale - retard, 0.0, 1.0);
}

// Ondulation ORGANIQUE : trois directions aux fréquences incommensurables
// (×1, ×1.73, ×2.61), donc un motif qui ne se répète jamais. Une onde unique
// et périodique donnait du carton ondulé ; c'est ce qui faisait « autocollant ».
float ondulation(vec2 q, float ph) {
  float f = uNbPlis * TAU;
  float a = sin(dot(q, vec2( 0.98,  0.20)) * f              + ph);
  float b = sin(dot(q, vec2(-0.34,  0.94)) * f * 1.73 + 1.7 + ph * 1.31);
  float g = sin(dot(q, vec2( 0.72, -0.69)) * f * 2.61 + 3.4 + ph * 0.77);
  float doux = (a + (b * 0.55 + g * 0.32) * uDesordre)
             / (1.0 + 0.87 * uDesordre);
  float franc = triangle(q.x * uNbPlis + ph / TAU) * 2.0 - 1.0;
  return mix(doux, franc, uArete) * 0.5;
}

// Champ de déplacement : hauteur du tissu en un point, à un avancement donné.
float champ(vec2 uv, out float pliLocal) {
  vec2 dir = vec2(cos(uAngle), sin(uAngle));
  vec2 c   = uv - 0.5;

  float local = avancementLocal(c, dir);

  // Dépassement du easing au-delà de 1 (rebond d'une Bézier dont y2 > 1). Il est
  // GLOBAL — il ne dépend pas de la position dans la feuille, contrairement au
  // balayage — et se retranche au reste à plier : le tissu passe DE L'AUTRE CÔTÉ
  // de l'état à plat, où le pli s'inverse, puis revient.
  // Sans ce terme le clamp de avancementLocal écrêterait tout dépassement à
  // « à plat » : la courbe rebondirait sans que rien ne se voie à l'écran.
  float rebond = max(0.0, uProgress - 1.0) * uAmpliRebond;
  float reste  = (1.0 - local) - rebond;

  // Résorption douce (exposant < 2) : le tissu reste vivant longtemps puis
  // s'éteint sans à-coup, au lieu de s'écraser d'un coup comme une décalque.
  // Le SIGNE est conservé — c'est lui qui inverse le pli pendant le rebond.
  pliLocal = sign(reste) * pow(min(abs(reste), 1.0), 1.6);

  // La phase DÉRIVE pendant que le tissu se pose : les plis glissent au lieu de
  // seulement perdre en amplitude. Sans ça, la forme est figée et l'œil lit un
  // motif imprimé qu'on aplatit — pas une étoffe qui bouge.
  float ph = (1.0 - local) * uDerive * TAU;

  vec2 q = tourne(c, -uAngle);
  float h     = ondulation(q, ph);
  float rides = sin(q.y * uNbRides * TAU + q.x * uNbPlis * TAU * 0.7 + ph * 1.4) * 0.5;

  return (h + rides * uAmplitudeRides) * uAmplitude * pliLocal;
}

// Contraction de l'emprise par conservation de la longueur d'étoffe.
// Une feuille qui ondule dépense de la matière en Z : ce qu'elle gagne en
// relief, elle le perd au sol. Pour une sinusoïde d'amplitude A et de nombre
// d'onde k, la longueur d'arc vaut ≈ L·(1 + (A·k)²/4) ; à quantité d'étoffe
// constante, l'emprise se contracte de l'inverse de ce facteur.
// Conséquence voulue : le retrait SUIT l'amplitude. Des plis deux fois plus
// profonds rétractent la carte d'eux-mêmes, sans réglage séparé.
float contraction(float amplitude, float nbOndes) {
  float ak = 0.5 * amplitude * nbOndes * TAU;
  return 1.0 / (1.0 + ak * ak * 0.25 * uRetrait);
}

void main() {
  vUv = aPos;

  vec2 c = aPos - 0.5;

  float pliLocal;
  float h = champ(aPos, pliLocal);
  // Le fragment reçoit la VALEUR ABSOLUE : ombre, voile et iridescence mesurent
  // l'activité du pli, pas son sens. Un vPli négatif pendant le rebond
  // éclaircirait l'image et ferait déborder l'alpha au-delà de 1.
  vPli = abs(pliLocal);

  // Le retrait s'applique dans le REPÈRE DU PLI, sur les deux axes : celui des
  // plis primaires (x) et celui des rides secondaires (y), chacun avec sa propre
  // amplitude et sa propre fréquence. Puis retour au repère de la carte.
  vec2 q = tourne(c, -uAngle);
  float A = uAmplitude * pliLocal;
  q.x *= contraction(A, uNbPlis);
  q.y *= contraction(A * uAmplitudeRides, uNbRides);
  vec2 pos = tourne(q, uAngle);

  // Inclinaison résiduelle, résorbée elle aussi.
  pos = tourne(pos, radians(uRotation) * pliLocal);

  // Il SE DÉPOSE : il vient d'un peu plus près (donc plus grand) et d'un peu
  // plus haut, puis descend à sa place. Sans ce déplacement, l'affiche s'aplatit
  // sur place — le geste d'un sticker qu'on colle.
  pos *= 1.0 + pliLocal * uApproche;
  pos.y += pliLocal * uChute;

  // Normale par différences finies sur le champ — les plis ne se lisent que si
  // la lumière les accroche, donc la normale doit être juste.
  float e = 0.004, ignore;
  float hx = champ(aPos + vec2(e, 0.0), ignore) - champ(aPos - vec2(e, 0.0), ignore);
  float hy = champ(aPos + vec2(0.0, e), ignore) - champ(aPos - vec2(0.0, e), ignore);
  vNormal = normalize(vec3(-hx / (2.0 * e), -hy / (2.0 * e), 1.0));

  // Fuite manuelle : pas de matrice de projection pour un seul quad.
  // Le Z de la carte s'AJOUTE au relief du pli avant la division : la carte
  // s'éloigne ou s'avance pour de bon, et sa profondeur se compose avec celle
  // des plis au lieu de s'y superposer comme un simple facteur d'échelle.
  float z = h + uProfondeurZ * pliLocal;
  // Garde-fou : le dénominateur ne doit jamais s'annuler (réglages extrêmes,
  // le labo laisse pousser les curseurs loin).
  float persp = 1.0 / max(0.2, 1.0 - z * uPerspective);
  pos *= persp;

  gl_Position = vec4(pos * 2.0 * uAspect, 0.0, 1.0);
}`;

const FRAG = `#version 300 es
precision highp float;

in vec2  vUv;
in vec3  vNormal;
in float vPli;

uniform sampler2D uTex;
uniform vec2  uAspect;
uniform vec2  uUvScale, uUvOffset;   // recadrage « cover » de l'affiche
uniform float uTemps;
uniform vec2  uLumiere;
uniform float uOmbre, uEclat, uTranslucide;
uniform float uIriIntensite, uIriFresnel, uIriEchelle, uIriDecalage, uIriVitesse, uIriRepos;
uniform float uGrain, uOpacite, uRayon, uVoile;

out vec4 fragColor;

const float TAU = 6.283185307;

// Palette cosinus (Inigo Quilez) — un spectre continu en 4 constantes, sans
// texture de rampe ni table de correspondance.
vec3 spectre(float t) {
  return 0.5 + 0.5 * cos(TAU * (vec3(1.0) * t + vec3(0.0, 0.33, 0.67)));
}

float alea(vec2 p) {
  return fract(sin(dot(p, vec2(12.9898, 78.233))) * 43758.5453);
}

// Distance signée à un rectangle arrondi — reproduit le border-radius de la
// carte, que le canvas ne peut pas hériter en CSS pendant l'animation.
float boiteArrondie(vec2 p, vec2 demi, float r) {
  vec2 q = abs(p) - demi + r;
  return length(max(q, 0.0)) + min(max(q.x, q.y), 0.0) - r;
}

void main() {
  vec3 N = normalize(vNormal);
  vec3 L = normalize(vec3(uLumiere, 1.0));
  vec3 V = vec3(0.0, 0.0, 1.0);

  // (0,0) = coin haut-gauche de l'affiche, puis recadrage « cover ».
  vec2 uv = vec2(vUv.x, 1.0 - vUv.y) * uUvScale + uUvOffset;
  vec3 base = texture(uTex, uv).rgb;

  // Modelé : éclairage ENVELOPPANT (wrap) plutôt que Lambert dur. Un Lambert
  // tranche net à l'ombre et emboutit l'image comme un décalque en relief ; un
  // tissu fin diffuse la lumière et ne produit jamais cette arête d'ombre.
  float nl    = dot(N, L);
  // Normalisé pour culminer à 1.0 : le modelé ne fait qu'OMBRER. Un maximum
  // au-dessus de 1 éclaircirait les crêtes et délaverait l'affiche.
  float diff  = pow(nl * 0.5 + 0.5, 1.3);
  float ombre = mix(1.0, 0.55 + 0.45 * diff, uOmbre * vPli);

  // Translucidité : là où l'étoffe se détourne de la lumière, elle la laisse
  // passer et s'éclaire par l'arrière. C'est ce qui la rend légère plutôt
  // qu'épaisse.
  float dos = max(0.0, -nl);

  // Reflet sur les crêtes — exposant bas, donc large et doux.
  vec3  H    = normalize(L + V);
  float spec = pow(max(dot(N, H), 0.0), 24.0) * uEclat * vPli;

  vec3 col = base * ombre + base * dos * uTranslucide * vPli + spec;

  // ── Iridescence ──
  // Elle NAÎT DU MOUVEMENT : indexée sur l'angle rasant (donc sur la courbure
  // du pli) et pondérée par vPli, elle s'éteint d'elle-même quand le tissu
  // s'aplatit. uIriRepos laisse un voile résiduel si on en veut un.
  float fres  = pow(1.0 - abs(dot(N, V)), uIriFresnel);
  float t     = fres * uIriEchelle + dot(N, L) * 0.35 + uIriDecalage + uTemps * uIriVitesse;
  float force = uIriIntensite * mix(uIriRepos, 1.0, vPli) * fres;
  col += spectre(t) * force;

  // Grain — casse le lissé du dégradé, comme sur une impression.
  col += (alea(vUv * 512.0 + uTemps) - 0.5) * uGrain;

  // Masque : coins arrondis, en tenant compte du débordement du quad.
  vec2 p = (vUv - 0.5) * 2.0 * uAspect;
  float d = boiteArrondie(p, uAspect, uRayon * min(uAspect.x, uAspect.y) * 2.0);
  float aa = fwidth(d) * 1.5;
  float masque = 1.0 - smoothstep(-aa, aa, d);

  // Voile : presque diaphane en vol, le tissu s'opacifie en se posant. Une
  // opacité pleine du début à la fin est ce qui donne le poids du vinyle.
  float alpha = masque * uOpacite * mix(1.0, 1.0 - uVoile, vPli);

  fragColor = vec4(col, alpha);
}`;

// ── Plomberie WebGL ────────────────────────────────────────────────────────

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

/**
 * Crée un renderer de dépliage sur un <canvas>.
 * Renvoie `null` si WebGL2 est indisponible — l'appelant retombe alors sur
 * l'affichage <img> classique.
 */
export function creerCloth(canvas, params = {}) {
  const gl = canvas.getContext('webgl2', {
    alpha: true, antialias: true, premultipliedAlpha: false,
  });
  if (!gl) return null;

  let P = { ...CLOTH_DEFAUTS, ...params };
  let prog;
  try {
    prog = programme(gl);
  } catch (e) {
    console.warn('[cloth] shader non compilé', e);
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

  const U = {};
  for (const nom of [
    'uProgress', 'uAspect', 'uAngle', 'uNbPlis', 'uAmplitude', 'uArete',
    'uDesordre', 'uDerive', 'uTraineBord', 'uChute', 'uApproche', 'uProfondeurZ',
    'uAmpliRebond',
    'uNbRides', 'uAmplitudeRides', 'uRetrait', 'uBalayage', 'uPerspective',
    'uRotation', 'uTex', 'uUvScale', 'uUvOffset', 'uTemps', 'uLumiere', 'uOmbre', 'uEclat',
    'uTranslucide', 'uVoile',
    'uIriIntensite', 'uIriFresnel', 'uIriEchelle', 'uIriDecalage', 'uIriVitesse',
    'uIriRepos', 'uGrain', 'uOpacite', 'uRayon',
  ]) U[nom] = gl.getUniformLocation(prog, nom);

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
  let easing = EASINGS.sortie;
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

  // Le quad prend la forme du GABARIT (jamais celle de l'affiche), inscrit dans
  // le canvas à l'échelle 1/débordement — la marge restante laisse le pli
  // sortir du cadre sans être coupé.
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
    if (ri > rc) {                       // affiche plus large que le gabarit
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
    gl.uniform1f(U.uAngle, (P.angle * Math.PI) / 180);
    gl.uniform1f(U.uNbPlis, P.nbPlis);
    gl.uniform1f(U.uAmplitude, P.amplitude);
    gl.uniform1f(U.uArete, P.arete);
    gl.uniform1f(U.uDesordre, P.desordre);
    gl.uniform1f(U.uDerive, P.derive);
    gl.uniform1f(U.uAmpliRebond, P.ampliRebond);
    gl.uniform1f(U.uTraineBord, P.traineBord);
    gl.uniform1f(U.uChute, P.chute);
    gl.uniform1f(U.uApproche, P.approche);
    gl.uniform1f(U.uProfondeurZ, P.profondeurZ);
    gl.uniform1f(U.uNbRides, P.nbRides);
    gl.uniform1f(U.uAmplitudeRides, P.amplitudeRides);
    gl.uniform1f(U.uRetrait, P.retrait);
    gl.uniform1f(U.uBalayage, P.balayage);
    gl.uniform1f(U.uPerspective, P.perspective);
    gl.uniform1f(U.uRotation, P.rotation);
    gl.uniform1f(U.uTemps, temps);
    gl.uniform2f(U.uLumiere, P.lumiereX, P.lumiereY);
    gl.uniform1f(U.uOmbre, P.ombre);
    gl.uniform1f(U.uEclat, P.eclat);
    gl.uniform1f(U.uTranslucide, P.translucide);
    gl.uniform1f(U.uVoile, P.voile);
    gl.uniform1f(U.uIriIntensite, P.iriIntensite);
    gl.uniform1f(U.uIriFresnel, P.iriFresnel);
    gl.uniform1f(U.uIriEchelle, P.iriEchelle);
    gl.uniform1f(U.uIriDecalage, P.iriDecalage);
    gl.uniform1f(U.uIriVitesse, P.iriVitesse);
    gl.uniform1f(U.uIriRepos, P.iriRepos);
    gl.uniform1f(U.uGrain, P.grain);
    gl.uniform1f(U.uOpacite, P.opacite);
    gl.uniform1f(U.uRayon, P.rayon);

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

    /** Rejoue le dépliage. `courbe` : nom, [x1,y1,x2,y2] ou fonction. */
    jouer(courbe = 'sortie', fin = null) {
      easing = resoudreEasing(courbe);
      onFin = fin;
      debut = 0;
      progress = 0;
      if (raf) cancelAnimationFrame(raf);
      raf = requestAnimationFrame(boucle);
    },

    /**
     * Fige l'animation et positionne la progression à la main (scrub).
     * La borne haute dépasse 1 : c'est la zone de rebond, il faut pouvoir s'y
     * arrêter pour la juger.
     */
    figer(p) {
      if (raf) { cancelAnimationFrame(raf); raf = null; }
      progress = Math.min(1.6, Math.max(-0.4, p));
      dessiner();
    },

    /** Met à jour les paramètres à chaud. Reconstruit la grille si besoin. */
    regler(patch) {
      const avant = P.subdivisions;
      P = { ...P, ...patch };
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
