-- @Description Ensures that an INSERT while VACUUM drop phase does not leave
-- the segfile state inconsistent on master and the primary.
--
CREATE EXTENSION IF NOT EXISTS gp_inject_fault;

-- Helper function
CREATE or REPLACE FUNCTION wait_until_acquired_lock_on_rel (relname text, lmode text, gp_segment_id integer) RETURNS /*in func*/
bool AS $$ /*in func*/
declare /*in func*/
  result bool; /*in func*/
begin /*in func*/
  result := false; /*in func*/
  -- Wait until lock is acquired /*in func*/
  while result = false loop /*in func*/
    SELECT l.granted INTO result /*in func*/
     from pg_locks l, pg_class c /*in func*/
     where l.relation = c.oid /*in func*/
     and c.relname=relname /*in func*/
     and l.mode=lmode /*in func*/
     and l.gp_segment_id=gp_segment_id; /*in func*/
    if result = false then /*in func*/
      perform pg_sleep(0.1); /*in func*/
    end if; /*in func*/
  end loop; /*in func*/
  return result; /*in func*/
end; /*in func*/
$$ language plpgsql;

-- Create a table with partitions and generate holes for VACUUM
CREATE TABLE insert_while_ao_vacuum_drop (a int, b int) with (appendonly=true)
DISTRIBUTED BY (a)
PARTITION BY RANGE (b)
(START (1) END (2) EVERY (1));

INSERT INTO insert_while_ao_vacuum_drop VALUES (1, 1);
DELETE FROM insert_while_ao_vacuum_drop;

-- Suspend VACUUM drop phase on primary before it acquires lock on the relation
SELECT gp_inject_fault('compaction_before_segmentfile_drop', 'suspend', dbid) FROM gp_segment_configuration WHERE content = 0 AND role = 'p';
1&: VACUUM insert_while_ao_vacuum_drop;
SELECT gp_wait_until_triggered_fault('compaction_before_segmentfile_drop', 1, dbid) FROM gp_segment_configuration WHERE content = 0 AND role = 'p';

-- Do INSERT at the same time and make sure that it acquires the
-- RowExclusiveLock on the partition relation
SELECT wait_until_acquired_lock_on_rel('insert_while_ao_vacuum_drop_1_prt_1', 'RowExclusiveLock', content) FROM gp_segment_configuration WHERE content = -1 AND role = 'p';
2&: INSERT INTO insert_while_ao_vacuum_drop VALUES (1, 1);

-- Reset the fault and join both sessions
SELECT gp_inject_fault('compaction_before_segmentfile_drop', 'reset', dbid) FROM gp_segment_configuration WHERE content = 0 AND role = 'p';
1<:
2<:

-- Verify INSERT still works and doesn't fail on "ERROR cannot insert into
-- segno (1) for AO relid <XX> that is in state AOSEG_STATE_AWAITING_DROP"
INSERT INTO insert_while_ao_vacuum_drop VALUES (1, 1);
