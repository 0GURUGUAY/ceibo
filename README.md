# ceibo
Gestion de rutas por el Ceibo

## CEIBO Router V5 (base de données partagée)

La V5 permet de partager les routes entre plusieurs machines (Mac, iPad, etc.) via Supabase.

### 1) Créer un projet Supabase

- Créer un projet sur Supabase.
- Récupérer:
	- `Project URL`
	- `anon public key`

### 2) Créer la table SQL

Dans SQL Editor Supabase, exécuter:

```sql
create table if not exists public.ceibo_route_collections (
	project_key text primary key,
	routes jsonb not null default '[]'::jsonb,
	updated_at timestamptz not null default now()
);
```

### 3) Autoriser l'accès anon (usage perso simple)

Pour un usage personnel/test, ajoute une policy permissive:

```sql
alter table public.ceibo_route_collections enable row level security;

create policy "ceibo public read"
on public.ceibo_route_collections
for select
to anon
using (true);

create policy "ceibo public write"
on public.ceibo_route_collections
for insert
to anon
with check (true);

create policy "ceibo public update"
on public.ceibo_route_collections
for update
to anon
using (true)
with check (true);
```

### 4) Configurer dans l'app

Dans l'onglet **Routes**:

- coller `Supabase URL`
- coller `Supabase anon key`
- choisir `Espace partagé` (ex: `ceibo-main`)
- cliquer **Connecter cloud**

Ensuite les actions `Sauvegarder`, `Supprimer`, `Importer` synchronisent la base cloud.
Le bouton **Rafraîchir cloud** recharge les routes partagées.

### 5) Auth Email/Mot de passe (Supabase Auth) + whitelist SQL

L'app supporte maintenant l'authentification **Email + mot de passe** depuis l'onglet Cloud.

1. Dans Supabase → `Authentication` → `Providers`, activer **Email**.
2. (Optionnel) Activer/désactiver la confirmation email selon ton usage.
3. Dans SQL Editor, créer la table de whitelist:

```sql
create table if not exists public.allowed_users (
	email text primary key,
	created_at timestamptz not null default now()
);

alter table public.allowed_users enable row level security;

drop policy if exists "allowed_users_select_authenticated" on public.allowed_users;
create policy "allowed_users_select_authenticated"
on public.allowed_users
for select
to authenticated
using (true);
```

4. Ajouter les emails autorisés:

```sql
insert into public.allowed_users(email) values
	('ton.email@domaine.com'),
	('autre@domaine.com')
on conflict (email) do nothing;
```

5. Dans CEIBO (onglet Cloud):
	 - cliquer **Connecter cloud**,
	 - saisir email + mot de passe,
	 - utiliser **Créer compte** ou **Se connecter email**.

Si la table `allowed_users` n'existe pas, l'app laisse passer l'authentification (mode sans whitelist).

## Roadmap 05/03/2026

### Livré dans cette itération

- Authentification utilisateur via **OAuth Google** (Supabase Auth) depuis l'onglet Cloud.
- **Journal navigation**:
	- enregistrement GPS continu (positions + vitesse),
	- capture inclinaison (device orientation iPad),
	- nuage de points vitesse vs inclinaison.
- **Livre moteur**:
	- entrée compteur heures,
	- carburant ajouté,
	- notes maintenance,
	- historique local.
- Onglet **Météo** dédié:
	- conditions actuelles,
	- impact navigation J+1/J+2/J+3,
	- pointeur météo positionnable sur la carte.

### Livré phase 2

- Synchronisation cloud Supabase enrichie:
	- routes,
	- photos waypoint,
	- journal navigation,
	- livre moteur.
- Push cloud des journaux en mode différé (debounce) pour éviter une écriture réseau à chaque point GPS.
- Bloc **Routes IA** dans l'onglet Routage:
	- profils `Safe`, `Équilibré`, `Performance`,
	- scoring météo + temps + part moteur,
	- bouton `Appliquer ce profil` pour charger automatiquement départ + mode voile + temps de bord.

### Livré phase 3

- IA routage enrichie avec **variantes de trajectoires**:
	- route actuelle,
	- contournement nord,
	- contournement sud,
	- directe départ-arrivée (si applicable).
- Classement multi-scénarios selon météo, durée et moteur.
- Application en un clic d'une proposition IA (trajet + paramètres de routage).

### Observabilité IA et journal de bord enrichi

- Fenêtre temporaire de **trafic modèle en temps réel** pendant les calculs IA:
	- étapes d'évaluation affichées en direct,
	- fermeture automatique après fin de session,
	- fermeture manuelle possible.
- Journal navigation complété avec champs courants du jour de bord:
	- heure de quart,
	- équipage/quart,
	- cap,
	- vent direction/force,
	- état de mer,
	- voilure,
	- baromètre,
	- loch (NM),
	- événements/manœuvres.

### Étapes suivantes proposées

1. Synchroniser les journaux navigation + moteur dans Supabase (multi-appareils).
2. Renforcer la gestion des rôles utilisateurs (propriétaire, équipier, lecture seule).
3. Routage avancé IA:
	 - générer plusieurs routes candidates,
	 - scorer selon vent/houle/pluie/risque,
	 - proposer la route “safe” vs “perf”.
4. Onglet météo enrichi:
	 - rafales, fronts, pression,
	 - alertes navigation personnalisées à 48-72h.
