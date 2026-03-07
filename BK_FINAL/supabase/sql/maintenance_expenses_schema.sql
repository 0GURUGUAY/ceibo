-- CEIBO V5 - Maintenance expenses migration
-- Creates dedicated table for invoices/expenses used by the FACTURES tab.

create table if not exists public.maintenance_expenses (
    id uuid primary key default gen_random_uuid(),
    project_id uuid not null references public.projects(id) on delete cascade,
    creator_email text not null,
    creator_name text,
    invoice_name text not null default '',
    invoice_data_url text not null default '',
    invoice_mime_type text not null default '',
    invoice_size_bytes bigint not null default 0,
    invoice_documents jsonb not null default '[]'::jsonb,
    expense_date date not null default current_date,
    supplier_name text not null default '',
    supplier_iban text not null default '',
    payer text not null default 'PATISSIER' check (payer in ('PATISSIER', 'KLENIK', 'OTRO')),
    payment_status text not null default 'pending' check (payment_status in ('new', 'pending', 'planned', 'paid')),
    total_amount double precision not null default 0,
    currency text not null default 'EUR',
    lines jsonb not null default '[]'::jsonb,
    note text not null default '',
    ai_comment text not null default '',
    post_comment text not null default '',
    scanned_text text not null default '',
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

-- If a legacy table exists with id as text, convert it to uuid when values are compatible.
do $$
begin
    if exists (
        select 1
        from information_schema.columns
        where table_schema = 'public'
          and table_name = 'maintenance_expenses'
          and column_name = 'id'
          and data_type <> 'uuid'
    ) then
        begin
            alter table public.maintenance_expenses
                alter column id type uuid using id::uuid;
        exception when others then
            raise exception 'maintenance_expenses.id ne peut pas etre converti en uuid. Nettoie/convertis les valeurs id legacy puis relance la migration.';
        end;
    end if;
end
$$;

-- If table existed from older versions, ensure required columns exist.
alter table public.maintenance_expenses
    add column if not exists project_id uuid references public.projects(id) on delete cascade,
    add column if not exists creator_email text,
    add column if not exists creator_name text,
    add column if not exists invoice_name text,
    add column if not exists invoice_data_url text,
    add column if not exists invoice_mime_type text,
    add column if not exists invoice_size_bytes bigint,
    add column if not exists invoice_documents jsonb,
    add column if not exists expense_date date,
    add column if not exists supplier_name text,
    add column if not exists supplier_iban text,
    add column if not exists payer text,
    add column if not exists payment_status text,
    add column if not exists total_amount double precision,
    add column if not exists currency text,
    add column if not exists lines jsonb,
    add column if not exists note text,
    add column if not exists ai_comment text,
    add column if not exists post_comment text,
    add column if not exists scanned_text text,
    add column if not exists created_at timestamptz,
    add column if not exists updated_at timestamptz;

-- Normalize defaults expected by app payloads.
alter table public.maintenance_expenses
    alter column invoice_name set default '',
    alter column invoice_data_url set default '',
    alter column invoice_mime_type set default '',
    alter column invoice_size_bytes set default 0,
    alter column invoice_documents set default '[]'::jsonb,
    alter column expense_date set default current_date,
    alter column supplier_name set default '',
    alter column supplier_iban set default '',
    alter column payer set default 'PATISSIER',
    alter column payment_status set default 'pending',
    alter column total_amount set default 0,
    alter column currency set default 'EUR',
    alter column lines set default '[]'::jsonb,
    alter column note set default '',
    alter column ai_comment set default '',
    alter column post_comment set default '',
    alter column scanned_text set default '',
    alter column created_at set default now(),
    alter column updated_at set default now();

update public.maintenance_expenses
set
    invoice_name = coalesce(invoice_name, ''),
    invoice_data_url = coalesce(invoice_data_url, ''),
    invoice_mime_type = coalesce(invoice_mime_type, ''),
    invoice_size_bytes = coalesce(invoice_size_bytes, 0),
    invoice_documents = coalesce(invoice_documents, '[]'::jsonb),
    expense_date = coalesce(expense_date, current_date),
    supplier_name = coalesce(supplier_name, ''),
    supplier_iban = coalesce(supplier_iban, ''),
    payer = case
        when upper(coalesce(payer, '')) = 'KLENIK' then 'KLENIK'
        when upper(coalesce(payer, '')) = 'OTRO' then 'OTRO'
        when upper(coalesce(payer, '')) = 'PATISSIER' then 'PATISSIER'
        else 'PATISSIER'
    end,
    payment_status = case
        when payment_status in ('new', 'pending', 'planned', 'paid') then payment_status
        else 'pending'
    end,
    total_amount = coalesce(total_amount, 0),
    currency = coalesce(nullif(currency, ''), 'EUR'),
    lines = coalesce(lines, '[]'::jsonb),
    note = coalesce(note, ''),
    ai_comment = coalesce(ai_comment, ''),
    post_comment = coalesce(post_comment, ''),
    scanned_text = coalesce(scanned_text, ''),
    created_at = coalesce(created_at, now()),
    updated_at = coalesce(updated_at, now());

create index if not exists maintenance_expenses_creator_project_idx
    on public.maintenance_expenses(creator_email, project_id);

create index if not exists maintenance_expenses_date_idx
    on public.maintenance_expenses(expense_date desc);

create index if not exists maintenance_expenses_supplier_idx
    on public.maintenance_expenses(supplier_name);

-- Ensure checks also exist for legacy pre-created tables.
alter table public.maintenance_expenses
    drop constraint if exists maintenance_expenses_payer_check,
    drop constraint if exists maintenance_expenses_payment_status_check;

alter table public.maintenance_expenses
    add constraint maintenance_expenses_payer_check
        check (payer in ('PATISSIER', 'KLENIK', 'OTRO')),
    add constraint maintenance_expenses_payment_status_check
        check (payment_status in ('new', 'pending', 'planned', 'paid'));

-- Keep updated_at current on updates.
create or replace function public.set_row_updated_at()
returns trigger
language plpgsql
as $$
begin
    new.updated_at = now();
    return new;
end;
$$;

drop trigger if exists trg_maintenance_expenses_updated_at on public.maintenance_expenses;
create trigger trg_maintenance_expenses_updated_at
before update on public.maintenance_expenses
for each row execute function public.set_row_updated_at();

alter table public.maintenance_expenses enable row level security;

-- Policies: each authenticated user reads/writes only their own rows.
drop policy if exists "maintenance_expenses_select_own" on public.maintenance_expenses;
create policy "maintenance_expenses_select_own"
on public.maintenance_expenses
for select
to authenticated
using (lower(creator_email) = lower(auth.jwt() ->> 'email'));

drop policy if exists "maintenance_expenses_insert_own" on public.maintenance_expenses;
create policy "maintenance_expenses_insert_own"
on public.maintenance_expenses
for insert
to authenticated
with check (lower(creator_email) = lower(auth.jwt() ->> 'email'));

drop policy if exists "maintenance_expenses_update_own" on public.maintenance_expenses;
create policy "maintenance_expenses_update_own"
on public.maintenance_expenses
for update
to authenticated
using (lower(creator_email) = lower(auth.jwt() ->> 'email'))
with check (lower(creator_email) = lower(auth.jwt() ->> 'email'));

drop policy if exists "maintenance_expenses_delete_own" on public.maintenance_expenses;
create policy "maintenance_expenses_delete_own"
on public.maintenance_expenses
for delete
to authenticated
using (lower(creator_email) = lower(auth.jwt() ->> 'email'));
