CREATE EXTENSION IF NOT EXISTS gp_inject_fault;
create language plpythonu;

-- Helper function
CREATE or REPLACE FUNCTION wait_for_segment_status_update(db_id smallint, state text, m text)
RETURNS bool AS
$$
declare
retries int; /* in func */
begin /* in func */
  retries := 1200; /* in func */
  loop /* in func */
    if (select count(*) = 1 from gp_segment_configuration where dbid = db_id and status = state and mode = m) then /* in func */
      return true; /* in func */
    end if; /* in func */
    if retries <= 0 then /* in func */
      return false; /* in func */
    end if; /* in func */
    perform pg_sleep(0.1); /* in func */
    retries := retries - 1; /* in func */
  end loop; /* in func */
end; /* in func */
$$ language plpgsql;

--
-- pg_ctl:
--   datadir: data directory of process to target with `pg_ctl`
--   command: commands valid for `pg_ctl`
--   command_mode: modes valid for `pg_ctl -m`
--
create or replace function pg_ctl(datadir text, command text, command_mode text default 'immediate')
returns text as $$
    import subprocess
    if command == 'promote':
        cmd = 'pg_ctl promote -D %s' % datadir
    elif command in ('stop', 'restart'):
        cmd = 'pg_ctl -l postmaster.log -D %s ' % datadir
        cmd = cmd + '-w -m %s %s' % (command_mode, command)
    else:
        return 'Invalid command input'

    return subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True).replace('.', '')
$$ language plpythonu;

CREATE or REPLACE FUNCTION gp_add_segment_mirror_for_content0()
RETURNS int AS
$$
declare
content_id int2;
db_id int2;
host_name text;
addr text;
p int4;
rp int4;
fs_name text;
datadir text;
fsmap text[][];
result int2;
begin
  select contentid, dbid, hostname, address, port, rep_port, fsname, fselocation into content_id, db_id, host_name, addr, p, rp, fs_name, datadir from add_mirror_args;
  fsmap := array[array[fs_name, datadir]::text[]];
  -- Timeout when the dispatcher polls results from QEs
  perform gp_inject_fault_new('dispatch_result_poll', 'skip', 1);
  -- Add mirror back
  select into result gp_add_segment_mirror(content_id, host_name, addr, p, rp, fsmap);
  -- Mark the mirror as down as gprecoverseg would do, without this the fts probe process will fail
  set allow_system_table_mods='dml';
  update pg_catalog.gp_segment_configuration set mode = 'r', status = 'd' where dbid = db_id;
  reset allow_system_table_mods;
  return result;
end;
$$ language plpgsql;

-- No segment down
select count(*) from gp_segment_configuration where status = 'd';

-- Record args for add mirror before removing the mirror
create temp table add_mirror_args (contentid int2, dbid int2, hostname text, address text, port int4, rep_port int4, fsname text, fselocation text);
insert into add_mirror_args(contentid, dbid, hostname, address, port, rep_port, fsname, fselocation)
select content, dbid, hostname, address, port, replication_port, fsname, fselocation
from gp_segment_configuration
join pg_filespace on dbid = 5 -- content 0
join pg_filespace_entry on fsedbid = dbid;

-- Take down a mirror
SELECT pg_ctl(fselocation, 'stop') from pg_filespace_entry where fsedbid = 5;
select wait_for_segment_status_update(5::smallint, 'd'::text, 's'::text);
select dbid, content, role, preferred_role, mode, status from gp_segment_configuration;

-- Timeout when the dispatcher polls results from QEs
select gp_inject_fault_new('dispatch_result_poll', 'skip', 1);

-- Remove segment mirror should work and deadlock should not happen
select gp_remove_segment_mirror(0::int2);

-- The mirror should be removed
select dbid, content, role, preferred_role, mode, status from gp_segment_configuration;

-- Reset the fault
SELECT gp_inject_fault('dispatch_result_poll', 'reset', 1);

-- Add segment mirror should also work and deadlock should not happen
select gp_add_segment_mirror_for_content0();

-- Mirror should be added back
select dbid, content, role, preferred_role, mode, status from gp_segment_configuration;

-- post test cleanup
-- start_ignore
\! gprecoverseg -aF
-- end_ignore

select wait_for_segment_status_update(5::smallint, 'u'::text, 's'::text);
select dbid, content, role, preferred_role, mode, status from gp_segment_configuration;
