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
